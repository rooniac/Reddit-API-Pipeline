import re
import json
import os
import time
import psycopg2
import psycopg2.pool
import psycopg2.extras
import nltk
from jedi.api.refactoring import inline
from nltk.sentiment.vader import SentimentIntensityAnalyzer
from textblob import TextBlob
import threading
from concurrent.futures import ThreadPoolExecutor
import numpy as np
from collections import Counter, defaultdict

from src.utils.config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
from src.utils.logger import setup_logger

# Thiết lập logger
logger = setup_logger(__name__, "logs/sentiment_analyzer.log")

# Tạo thread-local storage cho các resource dùng chung
thread_local = threading.local()

class SentimentAnalyzer:
    """Class phân tích tình cảm từ dữ liệu Reddit"""
    def __init__(self, min_conn=3, max_conn=10):
        """
        Khởi tạo SentimentAnalyzer với connection pool

        Args:
            min_conn (int): Số kết nối tối thiểu trong pool
            max_conn (int): Số kết nối tối đa trong pool
        """
        # Đảm bảo tất cả tài nguyên NLTK được tải đầy đủ
        self._download_nltk_resources()

        # Tạo VADER analyzer cho thread chính
        self.main_vader = SentimentIntensityAnalyzer()

        # Tải bộ từ điển tùy chỉnh cho lĩnh vực kỹ thuật
        self.tech_sentiment_dict = self._load_tech_sentiment_dict()

        # Cập nhật từ điển VADER với từ điển tùy chỉnh
        if self.tech_sentiment_dict:
            self.main_vader.lexicon.update(self.tech_sentiment_dict)

        # Tải danh sách công nghệ
        self.technologies = self._load_technologies_list()

        # Cache cho từ khóa công nghệ và kỹ thuật
        self.tech_terms = self._prepare_tech_terms()

        try:
            self.connection_pool = psycopg2.pool.ThreadedConnectionPool(
                min_conn,
                max_conn,
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            logger.info(f"Đã khởi tạo connection pool (min={min_conn}, max={max_conn})")
        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo connection pool: {str(e)}")
            raise

        # Khởi tạo cache cho kết quả phân tích
        self.sentiment_cache = {}
        self.cache_lock = threading.Lock()

        logger.info("SentimentAnalyzer đã được khởi tạo")

    def _get_vader(self):
        """
            Lấy VADER SentimentIntensityAnalyzer cho thread hiện tại

            Returns:
                SentimentIntensityAnalyzer: Instance cho thread hiện tại
        """
        if not hasattr(thread_local, 'vader'):
            try:
                thread_local.vader = SentimentIntensityAnalyzer()
                # Cập nhật từ điển cho thread
                if self.tech_sentiment_dict:
                    thread_local.vader.lexicon.update(self.tech_sentiment_dict)
            except Exception as e:
                logger.warning(f"Không thể tạo VADER riêng cho thread, sử dụng VADER chính: {str(e)}")
                return self.main_vader
        return thread_local.vader

    def _download_nltk_resources(self):
        """Tải các tài nguyên NLTK cần thiết"""
        resources = ['vader_lexicon', 'punkt', 'stopwords', 'wordnet']

        for resource in resources:
            try:
                # Kiểm tra xem resource đã được tải chưa
                if resource == 'vader_lexicon':
                    path = 'sentiment/vader_lexicon'
                elif resource == 'punkt':
                    path = 'tokenizers/punkt'
                else:
                    path = f'corpora/{resource}'

                try:
                    nltk.data.find(path)
                except LookupError:
                    logger.info(f"Tải xuống tài nguyên NLTK: {resource}")
                    nltk.download(resource)
            except Exception as e:
                logger.error(f"Lỗi khi tải tài nguyên NLTK {resource}: {str(e)}")

        logger.info("Tải xuống tài nguyên NLTK thành công")


    def _load_tech_sentiment_dict(self):
        """
            Tải từ điển cảm xúc tùy chỉnh cho lĩnh vực data engineering

            Returns:
                dict: Từ điển cảm xúc tùy chỉnh
        """
        sentiment_file = "config/tech_sentiment.json"

        # Từ điển mặc định cho các thuật ngữ kỹ thuật
        default_dict = {
            "hadoop": 0.0,
            "spark": 0.0,
            "postgresql": 0.0,
            "mysql": 0.0,
            "data warehouse": 0.0,
            "etl": 0.0,
            "pipeline": 0.0,
            "bug": -2.0,
            "crash": -2.5,
            "error": -1.5,
            "efficient": 2.0,
            "scalable": 1.5,
            "fast": 1.8,
            "slow": -1.5,
            "robust": 1.5,
            "outdated": -1.2,
            "deprecated": -1.0,
            "buggy": -2.5,
            "intuitive": 1.5,
            "complex": -0.5,
            "simple": 1.0,
            "powerful": 2.0,
            "weak": -1.5,
            "stable": 1.8,
            "unstable": -2.0,
            "expensive": -1.0,
            "cheap": 0.5,
            "free": 1.0,
            "open source": 1.0,
            "proprietary": -0.5,
            "learning curve": -0.5,
            "documentation": 0.5,
            "community": 1.0,
            "support": 1.0,
            "performance": 0.5,
            "memory leak": -2.0,
            "resource intensive": -1.0,
            "lightweight": 1.2,
            "bloated": -1.5,
            "best practice": 1.5,
            "framework": 0.0,
            "library": 0.0,
            "tool": 0.0
        }

        # Tạo thư mục config nếu chưa tồn tại
        os.makedirs("config", exist_ok=True)

        try:
            # Nếu file không tồn tại hoặc rỗng, tạo mới
            if not os.path.exists(sentiment_file) or os.path.getsize(sentiment_file) == 0:
                with open(sentiment_file, 'w', encoding='utf-8') as f:
                    json.dump(default_dict, f, indent=4)
                logger.info(f"Đã tạo file từ điển cảm xúc tùy chỉnh: {sentiment_file}")
                return default_dict

            # Đọc từ file
            with open(sentiment_file, 'r', encoding='utf-8') as f:
                custom_dict = json.load(f)
            logger.info(f"Đã tải từ điển cảm xúc tùy chỉnh từ {sentiment_file}")
            return custom_dict

        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Lỗi khi tải từ điển cảm xúc tùy chỉnh: {str(e)}")
            logger.info("Sử dụng từ điển mặc định")

            # Ghi lại file mặc định
            try:
                with open(sentiment_file, 'w', encoding='utf-8') as f:
                    json.dump(default_dict, f, indent=4)
            except IOError:
                logger.error(f"Không thể ghi file {sentiment_file}")

            return default_dict

    def _load_technologies_list(self):
        """Tải danh sách các công nghệ data engineering từ file"""
        tech_file = "config/technologies.json"

        try:
            # Kiểm tra xem file đã tồn tại chưa
            if not os.path.exists(tech_file):
                logger.warning(f"Không tìm thấy file cấu hình {tech_file}")
                return {}

            # Đọc danh sách công nghệ
            with open(tech_file, 'r', encoding='utf-8') as f:
                tech_categories = json.load(f)

            logger.info(f"Đã tải danh sách công nghệ từ {tech_file}")
            return tech_categories

        except Exception as e:
            logger.error(f"Lỗi khi tải danh sách công nghệ: {str(e)}")
            return {}

    def _prepare_tech_terms(self):
        """
            Chuẩn bị danh sách các thuật ngữ công nghệ và các bí danh

            Returns:
                dict: Từ điển các thuật ngữ công nghệ và bí danh
        """
        tech_terms = {
            'all_terms': set(),  # Tất cả các thuật ngữ
            'mapping': {}  # Mapping từ bí danh sang tên chính
        }

        # Thêm các thuật ngữ từ danh sách công nghệ
        for category, techs in self.technologies.items():
            for tech in techs:
                name = tech['name'].lower()
                tech_terms['all_terms'].add(name)
                tech_terms['mapping'][name] = name

                # Thêm các bí danh
                for alias in tech.get('aliases', []):
                    if alias:
                        alias = alias.lower()
                        tech_terms['all_terms'].add(alias)
                        tech_terms['mapping'][alias] = name

        logger.debug(f"Đã chuẩn bị {len(tech_terms['all_terms'])} thuật ngữ công nghệ")
        return tech_terms

    def get_db_connection(self, max_retries=3, retry_delay=1):
        """
            Lấy kết nối từ connection pool với retry logic

            Args:
                max_retries (int): Số lần thử lại tối đa
                retry_delay (int): Độ trễ giữa các lần thử (giây)

            Returns:
                connection: Kết nối PostgreSQL
        """
        retries = 0
        while retries < max_retries:
            try:
                conn = self.connection_pool.getconn()
                return conn
            except Exception as e:
                retries += 1
                logger.warning(f"Lỗi khi lấy kết nối (lần {retries}): {str(e)}")
                if retries >= max_retries:
                    logger.error("Không thể lấy kết nối sau nhiều lần thử")
                    raise
                time.sleep(retry_delay)

    def return_db_connection(self, conn):
        """
                Trả lại kết nối vào connection pool

                Args:
                    conn: Kết nối PostgreSQL cần trả lại
                """
        if conn:
            self.connection_pool.putconn(conn)

    def clean_text(self, text):
        """
            Làm sạch văn bản trước khi phân tích tình cảm

            Args:
                text (str): Văn bản cần làm sạch

            Returns:
                str: Văn bản đã làm sạch
        """

        if not text or not isinstance(text, str):
            return ""

        code_blocks = re.findall(r'```.*?```', text, re.DOTALL)
        for block in code_blocks:
            text = text.replace(block, " CODE_BLOCK ")

        inline_code = re.findall(r'`.*?`', text)
        for code in inline_code:
            text = text.replace(code, " CODE_SNIPPET ")

        # Xử lý URLs và HTML tags
        text = re.sub(r'http\S+|www\S+|https\S+', ' URL ', text, flags=re.MULTILINE)
        text = re.sub(r'<.*?>', '', text)

        # Bảo tồn các thuật ngữ kỹ thuật đặc biệt
        tech_pattern = '|'.join(re.escape(term) for term in self.tech_terms['all_terms'] if len(term.split()) == 1)
        if tech_pattern:
            text = re.sub(f'({tech_pattern})', r' \1 ', text, flags=re.IGNORECASE)

        # Xử lý ký tự đặc biệt nhưng giữ lại dấu câu quan trọng
        text = re.sub(r'[^\w\s!?.,;:()]', ' ', text)

        # Xử lý emojis phổ biến
        text = re.sub(r':\)|:-\)', ' positive_emoji ', text)
        text = re.sub(r':\(|:-\(', ' negative_emoji ', text)

        # Loại bỏ dấu cách thừa
        text = re.sub(r'\s+', ' ', text).strip()

        return text

    def analyze_sentiment_ensemble(self, text):
        """
            Phân tích tình cảm sử dụng ensemble approach kết hợp VADER và TextBlob

            Args:
                text (str): Văn bản cần phân tích

            Returns:
                dict: Kết quả phân tích cảm xúc
        """
        if not text:
            return {
                "compound": 0,
                "pos": 0,
                "neg": 0,
                "neu": 1,
                "textblob_polarity": 0,
                "textblob_subjectivity": 0,
                "ensemble_score": 0
            }

        # Cache key cho kết quả phân tích
        cache_key = hash(text)

        # Kiểm tra trong cache
        with self.cache_lock:
            if cache_key in self.sentiment_cache:
                return self.sentiment_cache[cache_key]

        clean_text = self.clean_text(text)
        if not clean_text:
            return {
                "compound": 0,
                "pos": 0,
                "neg": 0,
                "neu": 1,
                "textblob_polarity": 0,
                "textblob_subjectivity": 0,
                "ensemble_score": 0
            }

        # Phân tích với VADER
        vader = self._get_vader()
        vader_scores = vader.polarity_scores(clean_text)

        # Phân tích với TextBlob
        blob = TextBlob(clean_text)
        textblob_polarity = blob.sentiment.polarity
        textblob_subjectivity = blob.sentiment.subjectivity

        # kết hợp vader và textblob (70% VADER, 30% TextBlob)
        ensemble_score = vader_scores["compound"] * 0.7 + textblob_polarity * 0.3

        # Chuẩn hóa điểm ensemble về phạm vi [-1, 1]
        if ensemble_score > 1:
            ensemble_score = 1
        elif ensemble_score < -1:
            ensemble_score = -1

        # Kết quả cuối cùng
        result = {
            "compound": vader_scores["compound"],
            "pos": vader_scores["pos"],
            "neg": vader_scores["neg"],
            "neu": vader_scores["neu"],
            "textblob_polarity": textblob_polarity,
            "textblob_subjectivity": textblob_subjectivity,
            "ensemble_score": ensemble_score
        }

        # Lưu vào cache
        with self.cache_lock:
            self.sentiment_cache[cache_key] = result

        return result

    def analyze_contextual_sentiment(self, text, tech_name=None):
        """
            Phân tích tình cảm theo ngữ cảnh của một công nghệ cụ thể

            Args:
                text (str): Văn bản cần phân tích
                tech_name (str, optional): Tên công nghệ cần phân tích ngữ cảnh

            Returns:
                dict: Kết quả phân tích cảm xúc theo ngữ cảnh
        """
        if not text:
            return {"score": 0, "context": "", "tech_name": tech_name}

        # Nếu không chỉ định công nghệ, phân tích cảm xúc toàn bộ văn bản
        if not tech_name:
            sentiment = self.analyze_sentiment_ensemble(text)
            return {
                "score": sentiment["ensemble_score"],
                "context": text,
                "tech_name": None
            }

        # Tách văn bản thành các câu
        sentences = re.split(r'(?<=[.!?])\s+', text)

        tech_sentences = []
        for sentence in sentences:
            if tech_name.lower() in sentence.lower():
                tech_sentences.append(sentence)

        if not tech_sentences:
            return {"score": 0, "context": "", "tech_name": tech_name}

        # Phân tích cảm xúc cho từng câu chứa công nghệ
        tech_context = " ".join(tech_sentences)
        sentiment = self.analyze_sentiment_ensemble(tech_context)

        return {
            "score": sentiment["ensemble_score"],
            "context": tech_context,
            "tech_name": tech_name
        }

    def extract_tech_sentiments(self, text):
        """
            Trích xuất cảm xúc đối với từng công nghệ trong văn bản

            Args:
                text (str): Văn bản cần phân tích

            Returns:
                dict: Từ điển các công nghệ và cảm xúc tương ứng
        """
        if not text:
            return {}

        mentioned_techs = set()
        text_lower = text.lower()

        # Công nghệ đơn từ
        for term in self.tech_terms['all_terms']:
            if term.lower() in text_lower:
                main_name = self.tech_terms['mapping'].get(term.lower(), term.lower())
                mentioned_techs.add(main_name)

        # công nghệ nhiều từ
        for term in self.tech_terms['all_terms']:
            if ' ' in term and term.lower() in text_lower:
                main_name = self.tech_terms['mapping'].get(term.lower(), term.lower())
                mentioned_techs.add(main_name)

        tech_sentiments = {}
        for tech in mentioned_techs:
            sentiment = self.analyze_contextual_sentiment(text, tech)
            tech_sentiments[tech] = sentiment

        return tech_sentiments

    def analyze_post_sentiment(self, post_id):
        """
            Phân tích tình cảm của một bài viết và cập nhật bảng post_analysis

            Args:
                post_id (str): ID của bài viết cần phân tích

            Returns:
                dict: Kết quả phân tích
        """
        conn = None
        cur = None
        try:
            conn = self.get_db_connection()
            cur = conn.cursor()

            cur.execute("""
                            SELECT title, text FROM reddit_data.posts WHERE post_id = %s
                        """, (post_id,))
            result = cur.fetchone()

            if not result:
                logger.warning(f"Không tìm thấy bài viết với ID: {post_id}")
                return None

            title, text = result
            full_text = f"{title} {text}" if text else title

            sentiment = self.analyze_sentiment_ensemble(full_text)
            tech_sentiments = self.extract_tech_sentiments(full_text)

            avg_tech_sentiment = 0
            if tech_sentiments:
                scores = [s["score"] for s in tech_sentiments.values()]
                avg_tech_sentiment = sum(scores) / len(scores) if scores else 0

            # Lấy phân tích hiện có của bài viết để bổ sung thông tin
            cur.execute("""
                    SELECT tech_mentioned FROM reddit_data.post_analysis WHERE post_id = %s
                """, (post_id,))
            existing_analysis = cur.fetchone()

            tech_mentioned = None
            if existing_analysis:
                tech_mentioned = existing_analysis[0]

            # Cập nhật bảng post_analysis
            cur.execute("""
                    INSERT INTO reddit_data.post_analysis (
                        post_id, sentiment_score
                    ) VALUES (%s, %s)
                    ON CONFLICT (post_id) 
                    DO UPDATE SET 
                        sentiment_score = EXCLUDED.sentiment_score,
                        processed_date = CURRENT_TIMESTAMP
                """, (post_id, sentiment["ensemble_score"]))

            conn.commit()

            # Trả về kết quả phân tích
            analysis_result = {
                'post_id': post_id,
                'sentiment_scores': sentiment,
                'tech_sentiments': tech_sentiments,
                'avg_tech_sentiment': avg_tech_sentiment
            }

            logger.debug(f"Đã phân tích tình cảm cho bài viết {post_id}")
            return analysis_result

        except Exception as e:
            logger.error(f"Lỗi khi phân tích tình cảm cho bài viết {post_id}: {str(e)}")
            if conn:
                conn.rollback()
            return None

        finally:
            if cur:
                cur.close()
            if conn:
                self.return_db_connection(conn)

    def analyze_post_batch(self, post_ids):
        """
            Phân tích tình cảm cho một loạt bài viết

            Args:
                post_ids (list): Danh sách ID bài viết cần phân tích

            Returns:
                int: Số lượng bài viết đã phân tích thành công
        """
        if not post_ids:
            return 0

        conn = None
        cur = None
        try:
            # Lấy kết nối từ pool
            conn = self.get_db_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            # Truy vấn nhiều bài viết cùng lúc
            placeholders = ','.join(['%s'] * len(post_ids))
            query = f"""
                SELECT post_id, title, text 
                FROM reddit_data.posts 
                WHERE post_id IN ({placeholders})
            """
            cur.execute(query, post_ids)
            posts = cur.fetchall()

            # Chuẩn bị batch để insert/update
            sentiment_results = []

            for post in posts:
                post_id = post['post_id']
                title = post['title']
                text = post['text']

                # Kết hợp tiêu đề và nội dung
                full_text = f"{title} {text}" if text else title

                # Phân tích tình cảm
                sentiment = self.analyze_sentiment_ensemble(full_text)

                # Thêm vào batch
                sentiment_results.append((
                    post_id,
                    sentiment["ensemble_score"]
                ))

            # Bulk insert/update
            if sentiment_results:
                psycopg2.extras.execute_batch(cur, """
                            INSERT INTO reddit_data.post_analysis (
                                post_id, sentiment_score
                            ) VALUES (%s, %s)
                            ON CONFLICT (post_id) 
                            DO UPDATE SET 
                                sentiment_score = EXCLUDED.sentiment_score,
                                processed_date = CURRENT_TIMESTAMP
                        """, sentiment_results)

                conn.commit()

            return len(sentiment_results)

        except Exception as e:
            logger.error(f"Lỗi khi phân tích batch bài viết: {str(e)}")
            if conn:
                conn.rollback()
            return 0

        finally:
            if cur:
                cur.close()
            if conn:
                self.return_db_connection(conn)

    def analyze_comment_sentiment(self, comment_id):
        """
            Phân tích tình cảm của một bình luận

            Args:
                comment_id (str): ID của bình luận cần phân tích

            Returns:
                dict: Kết quả phân tích
        """
        conn = None
        cur = None
        try:
            # Lấy kết nối từ pool
            conn = self.get_db_connection()
            cur = conn.cursor()

            # Lấy nội dung bình luận
            cur.execute("""
                        SELECT body FROM reddit_data.comments WHERE comment_id = %s
                    """, (comment_id,))
            result = cur.fetchone()

            if not result:
                logger.warning(f"Không tìm thấy bình luận với ID: {comment_id}")
                return None

            body = result[0]

            # Phân tích tình cảm
            sentiment = self.analyze_sentiment_ensemble(body)

            # Trích xuất cảm xúc đối với từng công nghệ
            tech_sentiments = self.extract_tech_sentiments(body)

            # Cập nhật bảng comment_analysis
            cur.execute("""
                        INSERT INTO reddit_data.comment_analysis (
                            comment_id, sentiment_score
                        ) VALUES (%s, %s)
                        ON CONFLICT (comment_id) 
                        DO UPDATE SET 
                            sentiment_score = EXCLUDED.sentiment_score,
                            processed_date = CURRENT_TIMESTAMP
                    """, (comment_id, sentiment["ensemble_score"]))

            conn.commit()

            # Trả về kết quả phân tích
            analysis_result = {
                'comment_id': comment_id,
                'sentiment_scores': sentiment,
                'tech_sentiments': tech_sentiments
            }

            logger.debug(f"Đã phân tích tình cảm cho bình luận {comment_id}")
            return analysis_result

        except Exception as e:
            logger.error(f"Lỗi khi phân tích tình cảm cho bình luận {comment_id}: {str(e)}")
            if conn:
                conn.rollback()
            return None

        finally:
            if cur:
                cur.close()
            if conn:
                self.return_db_connection(conn)

    def analyze_comment_batch(self, comment_ids):
        """
            Phân tích tình cảm cho một loạt bình luận

            Args:
                comment_ids (list): Danh sách ID bình luận cần phân tích

            Returns:
                int: Số lượng bình luận đã phân tích thành công
        """
        if not comment_ids:
            return 0

        conn = None
        cur = None
        try:
            # Lấy kết nối từ pool
            conn = self.get_db_connection()
            cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

            # Truy vấn nhiều bình luận cùng lúc
            placeholders = ','.join(['%s'] * len(comment_ids))
            query = f"""
                        SELECT comment_id, body 
                        FROM reddit_data.comments 
                        WHERE comment_id IN ({placeholders})
                    """
            cur.execute(query, comment_ids)
            comments = cur.fetchall()

            # Chuẩn bị batch để insert/update
            sentiment_results = []

            for comment in comments:
                comment_id = comment['comment_id']
                body = comment['body']

                # Phân tích tình cảm
                sentiment = self.analyze_sentiment_ensemble(body)

                # Thêm vào batch
                sentiment_results.append((
                    comment_id,
                    sentiment["ensemble_score"]
                ))
            # Bulk insert/update
            if sentiment_results:
                psycopg2.extras.execute_batch(cur, """
                        INSERT INTO reddit_data.comment_analysis (
                            comment_id, sentiment_score
                        ) VALUES (%s, %s)
                        ON CONFLICT (comment_id) 
                        DO UPDATE SET 
                            sentiment_score = EXCLUDED.sentiment_score,
                            processed_date = CURRENT_TIMESTAMP
                    """, sentiment_results)

                conn.commit()

            return len(sentiment_results)

        except Exception as e:
            logger.error(f"Lỗi khi phân tích batch bình luận: {str(e)}")
            if conn:
                conn.rollback()
            return 0

        finally:
            if cur:
                cur.close()
            if conn:
                self.return_db_connection(conn)

    def analyze_tech_sentiment(self, tech_name):
        """
            Phân tích tình cảm đối với một công nghệ cụ thể

            Args:
                tech_name (str): Tên công nghệ cần phân tích

            Returns:
                dict: Kết quả phân tích
        """
        conn = None
        cur = None
        try:
            # Lấy kết nối từ pool
            conn = self.get_db_connection()
            cur = conn.cursor()

            # Tìm các bài viết nhắc đến công nghệ này
            cur.execute("""
                        SELECT p.post_id, p.title, p.text, p.created_date
                        FROM reddit_data.posts p
                        JOIN reddit_data.post_analysis pa ON p.post_id = pa.post_id
                        WHERE %s = ANY(pa.tech_mentioned)
                    """, (tech_name,))

            posts = cur.fetchall()

            if not posts:
                logger.warning(f"Không tìm thấy bài viết nào nhắc đến {tech_name}")
                return None

            total_sentiment = 0
            post_count = 0
            positive_count = 0
            negative_count = 0
            neutral_count = 0
            context_sentiments = []

            # Phân tích tình cảm theo ngữ cảnh cho từng bài viết
            for post_id, title, text, created_date in posts:
                full_text = f"{title} {text}" if text else title

                # Phân tích tình cảm theo ngữ cảnh
                context_sentiment = self.analyze_contextual_sentiment(full_text, tech_name)
                sentiment_score = context_sentiment["score"]

                # Lưu lại ngữ cảnh và điểm
                context_sentiments.append({
                    'post_id': post_id,
                    'context': context_sentiment["context"],
                    'score': sentiment_score,
                    'created_date': created_date
                })

                # Cập nhật thống kê
                total_sentiment += sentiment_score
                post_count += 1

                if sentiment_score > 0.05:
                    positive_count += 1
                elif sentiment_score < -0.05:
                    negative_count += 1
                else:
                    neutral_count += 1

            # Điểm tình cảm trung bình
            avg_sentiment = total_sentiment / post_count if post_count > 0 else 0

            # Cập nhật bảng tech_trends
            cur.execute("""
                UPDATE reddit_data.tech_trends
                SET sentiment_avg = %s
                WHERE tech_name = %s
            """, (avg_sentiment, tech_name))

            conn.commit()

            # Phân tích xu hướng theo thời gian (nếu có đủ dữ liệu)
            time_trend = None
            if len(context_sentiments) >= 3:
                # Sắp xếp theo thời gian
                context_sentiments.sort(key=lambda x: x['created_date'])

                # Chia thành các giai đoạn (ví dụ: đầu, giữa, cuối)
                segment_size = len(context_sentiments) // 3

                # Tính điểm trung bình cho mỗi giai đoạn
                time_trend = {
                    'early': sum(x['score'] for x in context_sentiments[:segment_size]) / segment_size,
                    'mid': sum(x['score'] for x in context_sentiments[segment_size:2 * segment_size]) / segment_size,
                    'recent': sum(x['score'] for x in context_sentiments[2 * segment_size:]) / (
                                len(context_sentiments) - 2 * segment_size)
                }

            # Trả về kết quả phân tích
            analysis_result = {
                'tech_name': tech_name,
                'post_count': post_count,
                'avg_sentiment': avg_sentiment,
                'positive_count': positive_count,
                'negative_count': negative_count,
                'neutral_count': neutral_count,
                'sentiment_distribution': {
                    'positive': positive_count / post_count if post_count > 0 else 0,
                    'negative': negative_count / post_count if post_count > 0 else 0,
                    'neutral': neutral_count / post_count if post_count > 0 else 0
                },
                'time_trend': time_trend,
                'top_positive_contexts': sorted([c for c in context_sentiments if c['score'] > 0.05],
                                                key=lambda x: x['score'], reverse=True)[:3],
                'top_negative_contexts': sorted([c for c in context_sentiments if c['score'] < -0.05],
                                                key=lambda x: x['score'])[:3]
            }

            logger.debug(f"Đã phân tích tình cảm cho công nghệ {tech_name}")
            return analysis_result

        except Exception as e:
            logger.error(f"Lỗi khi phân tích tình cảm cho công nghệ {tech_name}: {str(e)}")
            if conn:
                conn.rollback()
            return None

        finally:
            if cur:
                cur.close()
            if conn:
                self.return_db_connection(conn)

    def analyze_all_posts_parallel(self, max_workers=4, batch_size=50, limit=None):
        """
            Phân tích tình cảm cho tất cả các bài viết song song

            Args:
                max_workers (int): Số lượng worker threads tối đa
                batch_size (int): Kích thước của mỗi batch xử lý
                limit (int, optional): Giới hạn số lượng bài viết cần phân tích

            Returns:
                int: Số lượng bài viết đã phân tích
        """
        conn = None
        cur = None
        try:
            conn = self.get_db_connection()
            cur = conn.cursor()

            query = """
                SELECT p.post_id
                FROM reddit_data.posts p
                LEFT JOIN reddit_data.post_analysis pa ON p.post_id = pa.post_id
                WHERE pa.sentiment_score IS NULL OR pa.post_id IS NULL
            """

            if limit:
                query += f" LIMIT {limit}"

            cur.execute(query)
            post_ids = [row[0] for row in cur.fetchall()]
            logger.info(f"Tìm thấy {len(post_ids)} bài viết cần phân tích tình cảm")

            if not post_ids:
                return 0

            batches = [post_ids[i:i + batch_size] for i in range(0, len(post_ids), batch_size)]
            logger.info(f"Chia thành {len(batches)} batch, mỗi batch khoảng {batch_size} bài viết")

            # Xử lý song song các batch
            total_processed = 0

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                batch_results = list(executor.map(self.analyze_post_batch, batches))

            total_processed = sum(batch_results)
            logger.info(f"Đã phân tích tình cảm cho {total_processed}/{len(post_ids)} bài viết")
            return total_processed

        except Exception as e:
            logger.error(f"Lỗi khi phân tích tình cảm cho tất cả bài viết: {str(e)}")
            return 0

        finally:
            if cur:
                cur.close()
            if conn:
                self.return_db_connection(conn)

    def analyze_all_posts(self, limit=None):
        """
            Phân tích tình cảm cho tất cả các bài viết (wrapper cho phiên bản song song)

            Args:
                limit (int, optional): Giới hạn số lượng bài viết cần phân tích

            Returns:
                int: Số lượng bài viết đã phân tích
            """
        # Sử dụng phiên bản song song với cấu hình mặc định
        return self.analyze_all_posts_parallel(limit=limit)

    def analyze_all_comments_parallel(self, max_workers=4, batch_size=100, limit=None):
        """
            Phân tích tình cảm cho tất cả các bình luận song song

            Args:
                max_workers (int): Số lượng worker threads tối đa
                batch_size (int): Kích thước của mỗi batch xử lý
                limit (int, optional): Giới hạn số lượng bình luận cần phân tích

            Returns:
                int: Số lượng bình luận đã phân tích
        """
        conn = None
        cur = None
        try:
            # Lấy danh sách bình luận cần phân tích
            conn = self.get_db_connection()
            cur = conn.cursor()

            query = """
                SELECT c.comment_id
                FROM reddit_data.comments c
                LEFT JOIN reddit_data.comment_analysis ca ON c.comment_id = ca.comment_id
                WHERE ca.sentiment_score IS NULL OR ca.comment_id IS NULL
            """

            if limit:
                query += f" LIMIT {limit}"

            cur.execute(query)
            comment_ids = [row[0] for row in cur.fetchall()]

            logger.info(f"Tìm thấy {len(comment_ids)} bình luận cần phân tích tình cảm")

            if not comment_ids:
                return 0

            # Chia thành các batch nhỏ hơn
            batches = [comment_ids[i:i + batch_size] for i in range(0, len(comment_ids), batch_size)]
            logger.info(f"Chia thành {len(batches)} batch, mỗi batch khoảng {batch_size} bình luận")

            # Xử lý song song các batch
            total_processed = 0

            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                batch_results = list(executor.map(self.analyze_comment_batch, batches))

            # Tính tổng số bình luận đã xử lý
            total_processed = sum(batch_results)

            logger.info(f"Đã phân tích tình cảm cho {total_processed}/{len(comment_ids)} bình luận")
            return total_processed

        except Exception as e:
            logger.error(f"Lỗi khi phân tích tình cảm cho tất cả bình luận: {str(e)}")
            return 0

        finally:
            if cur:
                cur.close()
            if conn:
                self.return_db_connection(conn)

    def update_tech_sentiment(self):
        """
            Cập nhật điểm tình cảm cho tất cả các công nghệ

            Returns:
                int: Số lượng công nghệ đã cập nhật
        """
        conn = None
        cur = None
        try:
            conn = self.get_db_connection()
            cur = conn.cursor()

            cur.execute("""
                            SELECT DISTINCT unnest(tech_mentioned)
                            FROM reddit_data.post_analysis
                            WHERE tech_mentioned IS NOT NULL
                        """)

            technologies = [row[0] for row in cur.fetchall()]

            logger.info(f"Tìm thấy {len(technologies)} công nghệ cần cập nhật tình cảm")

            count = 0
            for tech in technologies:
                result = self.analyze_tech_sentiment(tech)
                if result:
                    count += 1

                # Log tiến trình
                if count % 10 == 0:
                    logger.info(f"Đã cập nhật tình cảm cho {count}/{len(technologies)} công nghệ")

            logger.info(f"Hoàn thành cập nhật tình cảm cho {count} công nghệ")
            return count

        except Exception as e:
            logger.error(f"Lỗi khi cập nhật tình cảm cho công nghệ: {str(e)}")
            return 0

        finally:
            if cur:
                cur.close()
            if conn:
                self.return_db_connection(conn)

    def analyze_sentiment_trends(self, period_days=30, min_mentions=5):
        """
                Phân tích xu hướng cảm xúc theo thời gian

                Args:
                    period_days (int): Khoảng thời gian phân tích (ngày)
                    min_mentions (int): Số lần nhắc đến tối thiểu để xem xét

                Returns:
                    dict: Kết quả phân tích xu hướng
                """
        conn = None
        cur = None
        try:
            conn = self.get_db_connection()
            cur = conn.cursor()
            cur.execute("""
                WITH tech_sentiments AS (
                SELECT 
                    unnest(pa.tech_mentioned) as tech_name,
                    pa.sentiment_score,
                    p.created_date
                FROM 
                    reddit_data.post_analysis pa
                    JOIN reddit_data.posts p ON pa.post_id = p.post_id
                WHERE 
                    pa.tech_mentioned IS NOT NULL
                    AND pa.sentiment_score IS NOT NULL
                    AND p.created_date >= CURRENT_DATE - INTERVAL '%s DAY'
                )   
                SELECT 
                    tech_name,
                    DATE_TRUNC('week', created_date) as week_start,
                    AVG(sentiment_score) as avg_sentiment,
                    COUNT(*) as mention_count
                FROM 
                    tech_sentiments
                GROUP BY 
                    tech_name, week_start
                HAVING 
                    COUNT(*) >= %s
                ORDER BY 
                    tech_name, week_start
            """, (period_days, min_mentions))

            trends_data = cur.fetchall()

            trends = defaultdict(list)
            for tech_name, week_start, avg_sentiment, mention_count in trends_data:
                trends[tech_name].append({
                    'week_start': week_start,
                    'avg_sentiment': float(avg_sentiment),
                    'mention_count': mention_count
                })

            trend_results = {}
            for tech, data in trends.items():
                if len(data) <= 1:
                    continue

                data.sort(key=lambda x: x['week_start'])

                # Tính toán xu hướng (độ dốc của đường xu hướng)
                x = list(range(len(data)))
                y = [point['avg_sentiment'] for point in data]

                # Tính hệ số góc bằng phương pháp bình phương tối thiểu
                n = len(x)
                if n >= 2:
                    slope = (n * sum(x[i] * y[i] for i in range(n)) - sum(x) * sum(y)) / \
                            (n * sum(x[i] ** 2 for i in range(n)) - sum(x) ** 2)
                else:
                    slope = 0

                trend_type = 'increasing' if slope > 0.05 else 'decreasing' if slope < -0.05 else 'stable'

                trend_results[tech] = {
                    'data': data,
                    'trend_slope': slope,
                    'trend_type': trend_type,
                    'current_sentiment': data[-1]['avg_sentiment'],
                    'total_mentions': sum(point['mention_count'] for point in data)
                }

            logger.info(f"Đã phân tích xu hướng cảm xúc cho {len(trend_results)} công nghệ")
            return trend_results

        except Exception as e:
            logger.error(f"Lỗi khi phân tích xu hướng cảm xúc: {str(e)}")
            return {}

        finally:
            if cur:
                cur.close()
            if conn:
                self.return_db_connection(conn)

    def compare_tech_sentiment(self, tech_names):
        """
           So sánh cảm xúc giữa các công nghệ

           Args:
               tech_names (list): Danh sách tên công nghệ cần so sánh

           Returns:
               dict: Kết quả so sánh
       """
        if not tech_names or len(tech_names) < 2:
            return None

        tech_results = {}
        for tech in tech_names:
            analysis = self.analyze_tech_sentiment(tech)
            if analysis:
                tech_results[tech] = analysis

        ranked_techs = sorted(tech_results.items(),
                              key=lambda x: x[1]['avg_sentiment'],
                              reverse=True)

        # So sánh phân bố cảm xúc
        comparison = {
            'ranking': [{'tech': tech, 'avg_sentiment': data['avg_sentiment']}
                        for tech, data in ranked_techs],
            'sentiment_distributions': {tech: data['sentiment_distribution']
                                        for tech, data in tech_results.items()},
            'mention_counts': {tech: data['post_count']
                               for tech, data in tech_results.items()}
        }

        # Thêm phân tích so sánh cặp đôi
        comparison['pairwise_differences'] = {}
        tech_list = list(tech_results.keys())
        for i in range(len(tech_list)):
            for j in range(i + 1, len(tech_list)):
                tech1, tech2 = tech_list[i], tech_list[j]
                diff = tech_results[tech1]['avg_sentiment'] - tech_results[tech2]['avg_sentiment']
                comparison['pairwise_differences'][f"{tech1} vs {tech2}"] = diff

        return comparison

    def update_sentiment_dictionary(self, updates):
        """
                Cập nhật từ điển cảm xúc tùy chỉnh

                Args:
                    updates (dict): Từ điển các cập nhật (từ khóa và giá trị mới)

                Returns:
                    bool: Thành công hay không
                """
        if not updates:
            return False

        try:
            # Cập nhật từ điển hiện tại
            self.tech_sentiment_dict.update(updates)

            # Lưu vào file
            sentiment_file = "config/tech_sentiment.json"
            with open(sentiment_file, 'w', encoding='utf-8') as f:
                json.dump(self.tech_sentiment_dict, f, indent=4)

            # Cập nhật VADER lexicon
            self.main_vader.lexicon.update(updates)

            # Xóa cache để đảm bảo các phân tích mới sẽ sử dụng từ điển mới
            with self.cache_lock:
                self.sentiment_cache.clear()

            logger.info(f"Đã cập nhật {len(updates)} mục trong từ điển cảm xúc")
            return True

        except Exception as e:
            logger.error(f"Lỗi khi cập nhật từ điển cảm xúc: {str(e)}")
            return False

    def close(self):
        """Đóng kết nối PostgreSQL"""
        if hasattr(self, 'cur') and self.cur:
            self.cur.close()
        if hasattr(self, 'conn') and self.conn:
            self.conn.close()
        logger.info("Đã đóng kết nối PostgreSQL")























