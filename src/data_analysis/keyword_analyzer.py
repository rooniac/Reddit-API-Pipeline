import re
import json
import os
import time
import psycopg2
import psycopg2.pool
import psycopg2.extras
from collections import Counter
import nltk
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from nltk.stem import WordNetLemmatizer
from rapidfuzz import process, fuzz
from rapidfuzz.process import extract
from rapidfuzz.fuzz import partial_ratio
from concurrent.futures import ThreadPoolExecutor
import concurrent.futures
from sklearn.feature_extraction.text import TfidfVectorizer
import numpy as np
import threading
import itertools

from src.utils.config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
from src.utils.logger import setup_logger

# Thiết lập logger cho file
logger = setup_logger(__name__, "logs/keyword_analyzer.log")

# Tạo thread-local storage cho các resource dùng chung
thread_local = threading.local()


class KeywordAnalyzer:
    """ Class dùng để phân tích từ khóa và các chủ đề """

    def __init__(self, min_conn=5, max_conn=20):
        """
        Khởi tạo KeywordAnalyzer

        Args:
            min_conn (int): Số kết nối tối thiểu trong pool
            max_conn (int): Số kết nối tối đa trong pool
        """
        # Đảm bảo tất cả tài nguyên NLTK được tải đầy đủ một lần duy nhất
        self._download_nltk_resources()

        # Tải sẵn stopwords một lần cho thread chính
        self.stop_words = set(stopwords.words('english'))

        # Tải danh sách các công nghệ và kỹ năng đã biết từ file
        self.technologies = self._load_technology_list()
        self.skills = self._load_skill_list()

        # Khởi tạo connection pool
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

        # Cache cho tech và skill để tăng tốc độ lookup
        self.tech_cache = self._prepare_tech_cache()
        self.skill_cache = self._prepare_skill_cache()

        # Lock cho việc sử dụng TF-IDF Vectorizer
        self.tfidf_lock = threading.Lock()

        logger.info("KeywordAnalyzer đã được khởi tạo")

    def _get_lemmatizer(self):
        """
        Lấy WordNetLemmatizer cho thread hiện tại

        Returns:
            WordNetLemmatizer: Instance dành riêng cho thread hiện tại
        """
        if not hasattr(thread_local, 'lemmatizer'):
            try:
                thread_local.lemmatizer = WordNetLemmatizer()
            except Exception as e:
                # Fallback: sử dụng lemmatizer của luồng chính nếu không thể tạo mới
                logger.warning(f"Không thể tạo lemmatizer riêng cho thread, sử dụng luồng chính: {str(e)}")
                return self.main_lemmatizer
        return thread_local.lemmatizer

    def _download_nltk_resources(self):
        """ Tải các tài nguyên của NLTK cần thiết """
        import nltk
        from nltk.stem import WordNetLemmatizer

        resources = ['punkt', 'stopwords', 'wordnet']

        for resource in resources:
            try:
                if resource == 'wordnet':
                    # Kiểm tra và tải trực tiếp WordNet
                    try:
                        from nltk.corpus import wordnet
                        # Thử truy cập một hàm của wordnet để đảm bảo nó đã được tải
                        wordnet.synsets('test')
                    except LookupError:
                        nltk.download('wordnet')
                        # Sau khi tải, cần import lại để đảm bảo sử dụng đúng phiên bản
                        from importlib import reload
                        import nltk.corpus
                        reload(nltk.corpus)
                else:
                    # Kiểm tra và tải các tài nguyên khác
                    try:
                        nltk.data.find(f'corpora/{resource}' if resource != 'punkt' else f'tokenizers/{resource}')
                    except LookupError:
                        nltk.download(resource)
            except Exception as e:
                logger.error(f"Lỗi khi tải tài nguyên NLTK {resource}: {str(e)}")

        # Khởi tạo trước một lemmatizer cho luồng chính
        self.main_lemmatizer = WordNetLemmatizer()
        logger.info("Tải xuống tài nguyên NLTK thành công")

    def _prepare_tech_cache(self):
        """Chuẩn bị cache cho danh sách các công nghệ"""
        tech_cache = {
            'names': [],  # Danh sách tên chính
            'all_terms': [],  # Danh sách tất cả các tên và alias
            'mapping': {}  # Mapping từ alias sang tên chính
        }

        for category, techs in self.technologies.items():
            for tech in techs:
                name = tech['name']
                tech_cache['names'].append(name)
                tech_cache['all_terms'].append(name)
                tech_cache['mapping'][name] = name

                # Thêm các alias
                for alias in tech.get('aliases', []):
                    if alias:
                        tech_cache['all_terms'].append(alias)
                        tech_cache['mapping'][alias] = name

        return tech_cache

    def _prepare_skill_cache(self):
        """Chuẩn bị cache cho danh sách các kỹ năng"""
        skill_cache = {
            'names': [],  # Danh sách tên chính
            'all_terms': [],  # Danh sách tất cả các tên và alias
            'mapping': {}  # Mapping từ alias sang tên chính
        }

        for category, skills in self.skills.items():
            for skill in skills:
                name = skill['name']
                skill_cache['names'].append(name)
                skill_cache['all_terms'].append(name)
                skill_cache['mapping'][name] = name

                # Thêm các alias
                for alias in skill.get('aliases', []):
                    if alias:
                        skill_cache['all_terms'].append(alias)
                        skill_cache['mapping'][alias] = name

        return skill_cache

    def _load_technology_list(self):
        """Tải danh sách các công nghệ data engineering từ file"""
        tech_file = "config/technologies.json"

        # Danh sách mặc định các công nghệ DE phổ biến
        default_techs = {
            "big_data": [
                {"name": "hadoop", "aliases": ["hdp", "apache hadoop"], "weight": 0.8},
                {"name": "spark", "aliases": ["pyspark", "apache spark"], "weight": 0.9},
                {"name": "flink", "aliases": ["apache flink"], "weight": 0.7},
                {"name": "hive", "aliases": ["apache hive"], "weight": 0.6},
                {"name": "pig", "aliases": ["apache pig"], "weight": 0.5}
            ],
            "databases": [
                {"name": "postgresql", "aliases": ["postgres", "psql"], "weight": 0.9},
                {"name": "mysql", "aliases": ["mariadb"], "weight": 0.8},
                {"name": "mongodb", "aliases": ["mongo"], "weight": 0.7},
                {"name": "cassandra", "aliases": ["apache cassandra"], "weight": 0.6}
            ]
        }

        # Tạo thư mục config nếu chưa tồn tại
        os.makedirs("config", exist_ok=True)

        try:
            # Nếu file không tồn tại hoặc rỗng, tạo lại
            if not os.path.exists(tech_file) or os.path.getsize(tech_file) == 0:
                with open(tech_file, 'w', encoding='utf-8') as f:
                    json.dump(default_techs, f, indent=4)
                logger.info(f"Đã tạo lại file {tech_file} với danh sách các công nghệ mặc định")
                return default_techs

            # Đọc file
            with open(tech_file, 'r', encoding='utf-8') as f:
                technologies = json.load(f)
            logger.info(f"Đã tải danh sách công nghệ từ {tech_file}")
            return technologies

        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Lỗi khi tải danh sách công nghệ: {str(e)}")
            logger.info("Sử dụng danh sách công nghệ mặc định")

            # Ghi lại file mặc định
            try:
                with open(tech_file, 'w', encoding='utf-8') as f:
                    json.dump(default_techs, f, indent=4)
            except IOError:
                logger.error("Không thể ghi lại file technologies.json")

            return default_techs

    def _load_skill_list(self):
        """Tải danh sách các kỹ năng data engineering từ file"""
        skill_file = "config/skills.json"

        default_skills = {
            "technical": [
                {"name": "data modeling", "aliases": ["modeling", "data model"], "weight": 0.8},
                {"name": "etl", "aliases": ["extract transform load", "data pipeline"], "weight": 0.9},
                {"name": "database design", "aliases": ["db design", "data schema"], "weight": 0.6},
                {"name": "data warehousing", "aliases": ["data warehouse"], "weight": 0.7}
            ],
            "soft_skills": [
                {"name": "problem solving", "aliases": ["analytical thinking"], "weight": 0.7},
                {"name": "communication", "aliases": ["team communication"], "weight": 0.6},
                {"name": "teamwork", "aliases": ["collaboration"], "weight": 0.6}
            ],
            "certifications": [
                {"name": "aws certified", "aliases": ["aws cert", "amazon web services"], "weight": 0.8},
                {"name": "azure certified", "aliases": ["azure cert", "microsoft azure"], "weight": 0.7},
                {"name": "gcp certified", "aliases": ["gcp cert", "google cloud platform"], "weight": 0.7}
            ]
        }

        # Tạo thư mục config nếu chưa tồn tại
        os.makedirs("config", exist_ok=True)

        try:
            if not os.path.exists(skill_file) or os.path.getsize(skill_file) == 0:
                with open(skill_file, 'w', encoding='utf-8') as f:
                    json.dump(default_skills, f, indent=4)
                logger.info(f"Đã tạo lại file {skill_file} với danh sách các kỹ năng mặc định")
                return default_skills
            # Đọc file
            with open(skill_file, 'r', encoding='utf-8') as f:
                skills = json.load(f)
            logger.info(f"Đã tải danh sách kỹ năng từ {skill_file}")
            return skills

        except (json.JSONDecodeError, IOError) as e:
            logger.error(f"Lỗi khi tải danh sách kỹ năng: {str(e)}")
            logger.info("Sử dụng danh sách kỹ năng mặc định")

            # Ghi lại file mặc định
            try:
                with open(skill_file, 'w', encoding='utf-8') as f:
                    json.dump(default_skills, f, indent=4)
            except IOError:
                logger.error("Không thể ghi lại file skills.json")

            return default_skills

    def add_technology(self, name, category='custom', aliases=None, weight=0.5):
        """
        Thêm công nghệ mới vào danh sách
        """
        if category not in self.technologies:
            self.technologies[category] = []

        new_tech = {
            "name": name,
            "aliases": aliases or [],
            "weight": weight
        }

        self.technologies[category].append(new_tech)

        # Cập nhật cache
        self.tech_cache['names'].append(name)
        self.tech_cache['all_terms'].append(name)
        self.tech_cache['mapping'][name] = name

        for alias in aliases or []:
            if alias:
                self.tech_cache['all_terms'].append(alias)
                self.tech_cache['mapping'][alias] = name

        # Lưu lại vào file
        tech_file = "config/technologies.json"
        with open(tech_file, 'w', encoding='utf-8') as f:
            json.dump(self.technologies, f, indent=4)

        logger.info(f"Đã thêm công nghệ mới: {name}")

    def add_skill(self, name, category='technical', aliases=None, weight=0.5):
        """
        Thêm kỹ năng mới vào danh sách
        """
        if category not in self.skills:
            self.skills[category] = []

        new_skill = {
            "name": name,
            "aliases": aliases or [],
            "weight": weight
        }

        self.skills[category].append(new_skill)

        # Cập nhật cache
        self.skill_cache['names'].append(name)
        self.skill_cache['all_terms'].append(name)
        self.skill_cache['mapping'][name] = name

        for alias in aliases or []:
            if alias:
                self.skill_cache['all_terms'].append(alias)
                self.skill_cache['mapping'][alias] = name

        # Lưu lại vào file
        skill_file = "config/skills.json"
        with open(skill_file, 'w', encoding='utf-8') as f:
            json.dump(self.skills, f, indent=4)

        logger.info(f"Đã thêm kỹ năng mới: {name}")

    def preprocess_text(self, text):
        """
        Tiền xử lý văn bản để chuẩn bị cho phân tích.

        Args:
            text (str): Văn bản cần xử lý

        Returns:
            list: Danh sách các từ đã được xử lý
        """
        if not text or not isinstance(text, str):
            return []

        # Sử dụng generator để tiết kiệm bộ nhớ
        text = text.lower()
        text = re.sub(r'<.*?>', '', text)  # Loại bỏ HTML tags
        text = re.sub(r'http\S+|www\S+|https\S+', '', text)  # Loại bỏ URLs
        text = re.sub(r'[^a-zA-Z\s]', ' ', text)

        # Tách từ và lọc stopwords hiệu quả hơn
        tokens = (token for token in word_tokenize(text)
                  if token not in self.stop_words and len(token) > 2)

        # Lemmatization - Sử dụng lemmatizer thread-local
        lemmatizer = self._get_lemmatizer()
        processed_tokens = [lemmatizer.lemmatize(t) for t in tokens]

        return processed_tokens

    def extract_technologies(self, text):
        """
        Trích xuất các công nghệ được đề cập trong văn bản với fuzzy matching

        Args:
            text (str): Văn bản cần phân tích

        Returns:
            list: Danh sách các công nghệ được tìm thấy (đã chuẩn hóa)
        """
        if not text or not isinstance(text, str):
            return []

        text = text.lower()

        # Sử dụng rapidfuzz với danh sách từ cache
        try:
            matches = extract(
                text,
                self.tech_cache['all_terms'],
                scorer=fuzz.partial_ratio,
                score_cutoff=80
            )
        except Exception as e:
            logger.error(f"Lỗi khi trích xuất công nghệ: {str(e)}")
            return []

        # Chuẩn hóa kết quả, đảm bảo trả về tên chính thay vì alias
        # và loại bỏ trùng lặp
        found_technologies = set()
        for match in matches:
            term = match[0]
            if term in self.tech_cache['mapping']:
                found_technologies.add(self.tech_cache['mapping'][term])

        return list(found_technologies)

    def extract_skills(self, text):
        """
        Trích xuất các kỹ năng được đề cập trong văn bản với fuzzy matching

        Args:
            text (str): Văn bản cần phân tích

        Returns:
            list: Danh sách các kỹ năng được tìm thấy (đã chuẩn hóa)
        """
        if not text or not isinstance(text, str):
            return []

        text = text.lower()

        # Sử dụng rapidfuzz với danh sách từ cache
        try:
            matches = extract(
                text,
                self.skill_cache['all_terms'],
                scorer=fuzz.partial_ratio,
                score_cutoff=80
            )
        except Exception as e:
            logger.error(f"Lỗi khi trích xuất kỹ năng: {str(e)}")
            return []

        # Chuẩn hóa kết quả, đảm bảo trả về tên chính thay vì alias
        # và loại bỏ trùng lặp
        found_skills = set()
        for match in matches:
            term = match[0]
            if term in self.skill_cache['mapping']:
                found_skills.add(self.skill_cache['mapping'][term])

        return list(found_skills)

    def extract_n_gram(self, tokens, n=2):
        """
        Tạo n-grams từ danh sách token.

        Args:
            tokens (list): Danh sách các token
            n (int): Chiều dài của n-gram

        Returns:
            list: Danh sách các n-gram
        """
        if len(tokens) < n:
            return []

        return [' '.join(tokens[i:i + n]) for i in range(len(tokens) - n + 1)]

    def extract_topics_tfidf(self, posts_data, num_topics=5, top_terms=10):
        """
        Trích xuất chủ đề sử dụng TF-IDF

        Args:
            posts_data (list): Danh sách văn bản từ các bài đăng
            num_topics (int): Số lượng chủ đề cần trích xuất
            top_terms (int): Số lượng từ khóa hàng đầu cho mỗi chủ đề

        Returns:
            list: Danh sách các chủ đề với từ khóa
        """
        if not posts_data:
            return []

        try:
            top_topics = []

            for post in posts_data:
                # Tiền xử lý văn bản
                tokens = self.preprocess_text(post)

                # Tạo n-grams
                bigrams = self.extract_n_gram(tokens, 2)
                trigrams = self.extract_n_gram(tokens, 3)

                # Kết hợp tokens, bigrams và trigrams để tìm top terms
                all_terms = tokens + bigrams + trigrams
                term_counts = Counter(all_terms)

                # Lấy top terms
                top_terms_list = [term for term, count in term_counts.most_common(top_terms)]
                top_topics.append(top_terms_list)

            return top_topics

        except Exception as e:
            logger.error(f"Lỗi khi trích xuất chủ đề: {str(e)}")
            return []

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

    def analyze_post(self, post_id):
        """
        Phân tích một bài viết và cập nhật bảng post_analysis.

        Args:
            post_id (str): ID của bài viết cần phân tích

        Returns:
            dict: Kết quả phân tích
        """
        conn = None
        cur = None
        try:
            # Lấy kết nối từ pool
            conn = self.get_db_connection()
            cur = conn.cursor()

            # Lấy nội dung bài viết
            cur.execute("""
                SELECT title, text FROM reddit_data.posts WHERE post_id = %s
            """, (post_id,))
            result = cur.fetchone()

            if not result:
                logger.warning(f"Không tìm thấy bài viết với ID: {post_id}")
                return None

            title, text = result

            # Kết hợp tiêu đề và nội dung bài viết để phân tích
            full_text = f"{title} {text}" if text else title

            # Tiền xử lý văn bản
            tokens = self.preprocess_text(full_text)

            # Trích xuất thông tin
            technologies = self.extract_technologies(full_text)
            skills = self.extract_skills(full_text)
            word_count = len(tokens)
            unique_words = len(set(tokens))

            # Tạo n_grams
            bigrams = self.extract_n_gram(tokens, 2)
            trigrams = self.extract_n_gram(tokens, 3)

            # Tính TF-IDF localy cho bigrams và trigrams
            all_ngrams = bigrams + trigrams
            if all_ngrams:
                # Chuẩn bị từ điển đếm
                ngram_counts = Counter(all_ngrams)

                # Xác định top ngrams theo tần suất
                common_ngrams = ngram_counts.most_common(10)
                topics = [item[0] for item in common_ngrams]
            else:
                topics = []

            # Là câu hỏi hay không?
            is_question = '?' in title if title else False

            # Lưu kết quả vào bảng post_analysis
            cur.execute("""
                INSERT INTO reddit_data.post_analysis (
                    post_id, word_count, unique_words, tech_mentioned, skills_mentioned, 
                    is_question, topics
                ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (post_id) DO UPDATE SET
                    word_count = EXCLUDED.word_count,
                    unique_words = EXCLUDED.unique_words,
                    tech_mentioned = EXCLUDED.tech_mentioned,
                    skills_mentioned = EXCLUDED.skills_mentioned,
                    is_question = EXCLUDED.is_question,
                    topics = EXCLUDED.topics,
                    processed_date = CURRENT_TIMESTAMP
            """, (
                post_id,
                word_count,
                unique_words,
                technologies if technologies else None,
                skills if skills else None,
                is_question,
                topics if topics else None
            ))

            conn.commit()

            # Trả về kết quả phân tích
            analysis_result = {
                'post_id': post_id,
                'word_count': word_count,
                'unique_words': unique_words,
                'technologies': technologies,
                'skills': skills,
                'is_question': is_question,
                'topics': topics
            }

            logger.debug(f"Đã phân tích bài viết {post_id}")
            return analysis_result

        except Exception as e:
            logger.error(f"Lỗi khi phân tích bài viết {post_id}: {str(e)}")
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
        Phân tích một loạt bài viết cùng lúc

        Args:
            post_ids (list): Danh sách ID bài viết cần phân tích

        Returns:
            int: Số lượng bài viết đã xử lý thành công
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

            # Chuẩn bị dữ liệu cho phân tích
            post_texts = []
            post_data = {}  # Lưu trữ thông tin mỗi bài viết

            for post in posts:
                post_id = post['post_id']
                title = post['title']
                text = post['text']

                # Kết hợp tiêu đề và nội dung
                full_text = f"{title} {text}" if text else title
                post_texts.append(full_text)

                # Lưu trữ thông tin để dùng sau
                post_data[post_id] = {
                    'title': title,
                    'full_text': full_text
                }

            # Chuẩn bị batch để insert/update
            analysis_results = []

            for post in posts:
                post_id = post['post_id']
                post_info = post_data[post_id]

                # Tiền xử lý văn bản
                tokens = self.preprocess_text(post_info['full_text'])

                # Trích xuất thông tin
                technologies = self.extract_technologies(post_info['full_text'])
                skills = self.extract_skills(post_info['full_text'])
                word_count = len(tokens)
                unique_words = len(set(tokens))

                # Tính chủ đề từ bigrams và trigrams
                bigrams = self.extract_n_gram(tokens, 2)
                trigrams = self.extract_n_gram(tokens, 3)
                all_ngrams = bigrams + trigrams

                # Xác định topics
                if all_ngrams:
                    ngram_counts = Counter(all_ngrams)
                    topics = [item[0] for item in ngram_counts.most_common(10)]
                else:
                    topics = []

                # Là câu hỏi hay không?
                is_question = '?' in post_info['title'] if post_info['title'] else False

                # Thêm vào batch
                analysis_results.append((
                    post_id,
                    word_count,
                    unique_words,
                    technologies if technologies else None,
                    skills if skills else None,
                    is_question,
                    topics if topics else None
                ))

            # Bulk insert/update
            if analysis_results:
                psycopg2.extras.execute_batch(cur, """
                    INSERT INTO reddit_data.post_analysis (
                        post_id, word_count, unique_words, tech_mentioned, skills_mentioned, 
                        is_question, topics
                    ) VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT (post_id) DO UPDATE SET
                        word_count = EXCLUDED.word_count,
                        unique_words = EXCLUDED.unique_words,
                        tech_mentioned = EXCLUDED.tech_mentioned,
                        skills_mentioned = EXCLUDED.skills_mentioned,
                        is_question = EXCLUDED.is_question,
                        topics = EXCLUDED.topics,
                        processed_date = CURRENT_TIMESTAMP
                """, analysis_results)

                conn.commit()

            return len(analysis_results)

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

    def analyze_all_posts_parallel(self, max_workers=4, batch_size=50, limit=None):
        """
        Phân tích tất cả các bài viết song song với đa luồng và xử lý theo batch

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
            # Lấy danh sách bài viết cần phân tích
            conn = self.get_db_connection()
            cur = conn.cursor()

            query = """
                SELECT p.post_id
                FROM reddit_data.posts p
                LEFT JOIN reddit_data.post_analysis pa ON p.post_id = pa.post_id
                WHERE pa.post_id IS NULL
            """

            if limit:
                query += f" LIMIT {limit}"

            cur.execute(query)
            post_ids = [row[0] for row in cur.fetchall()]

            logger.info(f"Tìm thấy {len(post_ids)} bài viết chưa được phân tích")

            if not post_ids:
                return 0

            # Chia thành các batch nhỏ hơn
            batches = [post_ids[i:i + batch_size] for i in range(0, len(post_ids), batch_size)]
            logger.info(f"Chia thành {len(batches)} batch, mỗi batch khoảng {batch_size} bài viết")

            # Xử lý song song các batch
            total_processed = 0
            batch_results = []

            # Sử dụng phương pháp map thay vì submit để kiểm soát tốt hơn
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                batch_results = list(executor.map(self.analyze_post_batch, batches))

            # Tính tổng số bài viết đã xử lý
            total_processed = sum(batch_results)

            logger.info(f"Đã phân tích tổng cộng {total_processed}/{len(post_ids)} bài viết")
            return total_processed

        except Exception as e:
            logger.error(f"Lỗi khi phân tích tất cả bài viết: {str(e)}")
            return 0

        finally:
            if cur:
                cur.close()
            if conn:
                self.return_db_connection(conn)

    def analyze_all_posts(self, limit=None):
        """
            Phân tích tất cả các bài viết chưa được phân tích.
            Hàm wrapper cho analyze_all_posts_parallel để duy trì khả năng tương thích.

            Args:
                limit (int, optional): Giới hạn số lượng bài viết cần phân tích

            Returns:
                int: Số lượng bài viết đã phân tích
        """
        # Sử dụng phiên bản song song với cấu hình mặc định
        return self.analyze_all_posts_parallel(limit=limit)

    # def update_tech_trends(self):
    #     """
    #     Cập nhật bảng tech_trends dựa trên phân tích bài viết.
    #
    #     Returns:
    #         int: Số lượng xu hướng đã cập nhật
    #     """
    #     conn = None
    #     cur = None
    #     try:
    #         # Lấy kết nối từ pool
    #         conn = self.get_db_connection()
    #         cur = conn.cursor()
    #
    #         # Xóa dữ liệu cũ
    #         cur.execute("TRUNCATE TABLE reddit_data.tech_trends RESTART IDENTITY")
    #
    #         # Chèn dữ liệu mới từ phân tích post
    #         cur.execute("""
    #             WITH tech_mentions AS (
    #                 SELECT
    #                     unnest(pa.tech_mentioned) as tech_name,
    #                     DATE_TRUNC('week', p.created_date) as week_start,
    #                     p.subreddit_id
    #                 FROM
    #                     reddit_data.post_analysis pa
    #                     JOIN reddit_data.posts p ON pa.post_id = p.post_id
    #                 WHERE
    #                     pa.tech_mentioned IS NOT NULL
    #             )
    #             INSERT INTO reddit_data.tech_trends (
    #                 tech_name, mention_count, week_start, subreddit_id
    #             )
    #             SELECT
    #                 tech_name,
    #                 COUNT(*) as mention_count,
    #                 week_start,
    #                 subreddit_id
    #             FROM
    #                 tech_mentions
    #             GROUP BY
    #                 tech_name, week_start, subreddit_id
    #             ORDER BY
    #                 week_start, mention_count DESC
    #         """)
    #
    #         # Lấy số lượng trends đã tạo
    #         cur.execute("SELECT COUNT(*) FROM reddit_data.tech_trends")
    #         trend_count = cur.fetchone()[0]
    #
    #         conn.commit()
    #         logger.info(f"Đã cập nhật {trend_count} xu hướng công nghệ")
    #
    #         return trend_count
    #
    #     except Exception as e:
    #         logger.error(f"Lỗi khi cập nhật xu hướng công nghệ: {str(e)}")
    #         if conn:
    #             conn.rollback()
    #         return 0
    #
    #     finally:
    #         if cur:
    #             cur.close()
    #         if conn:
    #             self.return_db_connection(conn)

    def update_tech_trends(self):
        """
        Cập nhật bảng tech_trends dựa trên phân tích bài viết,
        sử dụng phương pháp cập nhật gia tăng với xử lý tối ưu cho bảng trống.

        Returns:
            int: Số lượng xu hướng đã cập nhật
        """
        conn = None
        cur = None
        try:
            conn = self.get_db_connection()
            cur = conn.cursor()

            cur.execute("SELECT COUNT(*) FROM reddit_data.tech_trends")
            table_empty = cur.fetchone()[0] == 0

            if table_empty:
                cur.execute("""
                    WITH tech_mentions AS (
                        SELECT 
                            unnest(pa.tech_mentioned) as tech_name,
                            DATE_TRUNC('week', p.created_date) as week_start,
                            p.subreddit_id
                        FROM 
                            reddit_data.post_analysis pa
                            JOIN reddit_data.posts p ON pa.post_id = p.post_id
                        WHERE 
                            pa.tech_mentioned IS NOT NULL
                    )
                    INSERT INTO reddit_data.tech_trends (
                        tech_name, mention_count, week_start, subreddit_id
                    )
                    SELECT 
                        tech_name,
                        COUNT(*) as mention_count,
                        week_start,
                        subreddit_id
                    FROM 
                        tech_mentions
                    GROUP BY 
                        tech_name, week_start, subreddit_id
                    ORDER BY 
                        week_start, mention_count DESC
                """)

                cur.execute("SELECT COUNT(*) FROM reddit_data.tech_trends")
                trend_count = cur.fetchone()[0]

                conn.commit()
                logger.info(f"Đã chèn {trend_count} xu hướng công nghệ vào bảng trống")

                return trend_count

            else:
                # Cập nhật gia tăng
                # Xác định khoảng thời gian có dữ liệu mới
                cur.execute("""
                    WITH last_update AS (
                        SELECT MAX(processed_date) as last_date
                        FROM reddit_data.tech_trends
                    )
                    SELECT 
                        MIN(DATE_TRUNC('week', p.created_date)) as min_week,
                        MAX(DATE_TRUNC('week', p.created_date)) as max_week
                    FROM 
                        reddit_data.post_analysis pa
                        JOIN reddit_data.posts p ON pa.post_id = p.post_id
                    WHERE 
                        pa.processed_date > COALESCE((SELECT last_date FROM last_update), '2013-01-01')
                        AND pa.tech_mentioned IS NOT NULL
                """)

                date_range = cur.fetchone()
                min_week, max_week = date_range[0], date_range[1]

                if min_week is None or max_week is None:
                    logger.info("Không có dữ liệu mới để cập nhật xu hướng công nghệ")
                    return 0

                # Xóa dữ liệu cũ trong khoảng thời gian cần cập nhật
                cur.execute("""
                    DELETE FROM reddit_data.tech_trends
                    WHERE week_start BETWEEN %s AND %s
                """, (min_week, max_week))

                # Chèn dữ liệu mới cho khoảng thời gian đã xóa
                cur.execute("""
                    WITH tech_mentions AS (
                        SELECT 
                            unnest(pa.tech_mentioned) as tech_name,
                            DATE_TRUNC('week', p.created_date) as week_start,
                            p.subreddit_id
                        FROM 
                            reddit_data.post_analysis pa
                            JOIN reddit_data.posts p ON pa.post_id = p.post_id
                        WHERE 
                            pa.tech_mentioned IS NOT NULL
                            AND DATE_TRUNC('week', p.created_date) BETWEEN %s AND %s
                    )
                    INSERT INTO reddit_data.tech_trends (
                        tech_name, mention_count, week_start, subreddit_id
                    )
                    SELECT 
                        tech_name,
                        COUNT(*) as mention_count,
                        week_start,
                        subreddit_id
                    FROM 
                        tech_mentions
                    GROUP BY 
                        tech_name, week_start, subreddit_id
                    ORDER BY 
                        week_start, mention_count DESC
                """, (min_week, max_week))

                cur.execute("""
                    SELECT COUNT(*) FROM reddit_data.tech_trends
                    WHERE week_start BETWEEN %s AND %s
                """, (min_week, max_week))

                trend_count = cur.fetchone()[0]

                conn.commit()
                logger.info(f"Đã cập nhật {trend_count} xu hướng công nghệ từ {min_week} đến {max_week}")

                return trend_count

        except Exception as e:
            logger.error(f"Lỗi khi cập nhật xu hướng công nghệ: {str(e)}")
            if conn:
                conn.rollback()
            return 0

        finally:
            if cur:
                cur.close()
            if conn:
                self.return_db_connection(conn)

    def close(self):
        """Đóng connection pool"""
        if hasattr(self, 'connection_pool'):
            self.connection_pool.closeall()
            logger.info("Đã đóng tất cả kết nối trong pool")