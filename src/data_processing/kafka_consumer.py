import json
import time
from kafka import KafkaConsumer
import psycopg2
from psycopg2.extras import execute_values

from src.utils.config import (
    KAFKA_BOOTSTRAP_SERVERS, KAFKA_POSTS_TOPIC, KAFKA_COMMENTS_TOPIC,
    POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
)
from src.utils.logger import setup_logger

# Thiết lập logger
logger = setup_logger(__name__, "logs/kafka_consumer.log")

class RedditDataConsumer:
    """
        Class tiêu thụ dữ liệu từ Kafka, xử lý và lưu vào PostgreSQL
    """

    def __init__(self, topics=None, group_id="reddit_data_group"):
        """
            Khởi tạo Kafka consumer và kết nối PostgreSQL

            Args:
                topics (list): Danh sách các Kafka topics cần theo dõi
                group_id (str): Consumer group ID
        """

        # Thiết lập kafka topics
        if topics is None:
            self.topics = [KAFKA_POSTS_TOPIC, KAFKA_COMMENTS_TOPIC]
        else:
            self.topics = topics

        self.group_id = group_id

        # Khởi tạo Kafka consumer
        logger.info(f"Khởi tạo Kafka consumer cho topics: {', '.join(self.topics)}")
        try:
            self.consumer = KafkaConsumer(
                *self.topics,
                bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
                auto_offset_reset = 'earliest',
                enable_auto_commit = True,
                group_id = self.group_id,
                value_deserializer = lambda x: json.loads(x.decode('utf-8'))
            )
            logger.info("Kafka consumer đã được khởi tạo thành công")

        except Exception as e:
            logger.error(f"Lỗi khi khởi tạo Kafka consumer: {str(e)}")
            raise

        # Kết nối đến PostgreSQL
        logger.info(f"Kết nối đến PostgreSQL database: {POSTGRES_DB} tại {POSTGRES_HOST}:{POSTGRES_PORT}")
        try:
            self.conn = psycopg2.connect(
                host=POSTGRES_HOST,
                port=POSTGRES_PORT,
                dbname=POSTGRES_DB,
                user=POSTGRES_USER,
                password=POSTGRES_PASSWORD
            )
            self.cur = self.conn.cursor()
            logger.info("Đã kết nối thành công đến PostgreSQL")
        except Exception as e:
            logger.error(f"Lỗi khi kết nối PostgreSQL: {str(e)}")
            raise

    def process_data(self):
        """
            Xử lý dữ liệu từ Kafka và lưu vào PostgreSQL
        """
        logger.info("Bắt đầu xử lý dữ liệu từ Kafka")

        try:
            # Duyệt qua từng message từ Kafka
            for message in self.consumer:
                topic = message.topic
                data = message.value

                try:
                    if topic == KAFKA_POSTS_TOPIC:
                        self._process_post(data)
                    elif topic == KAFKA_COMMENTS_TOPIC:
                        self._process_comment(data)

                    # Commit transaction sau khi xử lý mỗi tin nahwns
                    self.conn.commit()
                except Exception as e:
                    logger.error(f"Lỗi khi xử lý tin nhắn: {str(e)}")
                    self.conn.rollback() # Nếu có lỗi thì
        except KeyboardInterrupt:
            logger.info("Nhận được tín hiệu ngắt, dừng consumer")
        except Exception as e:
            logger.error(f"Lỗi không xác định trong quá trình xử lý: {str(e)}")
        finally:
            self.close()

    def _process_post(self, post_data):
        """
            Xử lý dữ liệu bài viết và lưu vào PostgreSQL

            Args:
                post_data (dict): Dữ liệu bài viết từ Kafka
        """

        logger.debug(f"Xử lý bài viết: {post_data.get('id')}")

        try:
            # Đảm bảo có subreddit tồn tại ở bảng subreddits
            self._ensure_subreddit_exists(post_data.get('subreddit'))

            # Get subreddit id
            self.cur.execute(
                "SELECT subreddit_id FROM reddit_data.subreddits WHERE name = %s",
                (post_data.get('subreddit'),)
            )
            result = self.cur.fetchone()
            subreddit_id = result[0] if result else None

            # Thêm hoặc cập nhật dữ liệu bài viết
            self.cur.execute("""
                INSERT INTO reddit_data.posts (
                    post_id, subreddit_id, title, text, url, author, score, upvote_ratio,
                    num_comments, created_utc, created_date, is_self, is_video,
                    over_18, permalink, link_flair_text, collected_utc
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (post_id)
                DO UPDATE SET
                    score = EXCLUDED.score,
                    upvote_ratio = EXCLUDED.upvote_ratio,
                    num_comments = EXCLUDED.num_comments,
                    collected_utc = EXCLUDED.collected_utc;
            """, (
                post_data.get('id'),
                subreddit_id,
                post_data.get('title'),
                post_data.get('text'),
                post_data.get('url'),
                post_data.get('author'),
                post_data.get('score'),
                post_data.get('upvote_ratio'),
                post_data.get('num_comments'),
                post_data.get('created_utc'),
                post_data.get('created_date'),
                post_data.get('is_self', False),
                post_data.get('is_video', False),
                post_data.get('over_18', False),
                post_data.get('permalink'),
                post_data.get('link_flair_text'),
                post_data.get('collected_utc')
            ))

            # Cập nhật bảng user_activity
            self._update_user_activity(post_data.get('author'), is_post=True)

            logger.debug(f"Đã lưu bài viết {post_data.get('id')} vào PostgreSQL")

        except Exception as e:
            logger.error(f"Lỗi khi xử lý bài viết {post_data.get('id')}: {str(e)}")
            raise

    def _process_comment(self, comment_data):
        """
            Xử lý dữ liệu bình luận và lưu vào PostgreSQL

            Args:
                comment_data (dict): Dữ liệu bình luận từ Kafka
        """

        logger.debug(f"Xử lý bình luận: {comment_data.get('id')}")

        try:
            # Bổ sung logic để fix lỗi insert or update comment
            post_id = comment_data.get('post_id')
            self.cur.execute(
                "SELECT COUNT(*) FROM reddit_data.posts WHERE post_id = %s",
                (post_id,)
            )
            post_exists = self.cur.fetchone()[0] > 0

            if not post_exists:
                logger.warning(f"Bỏ qua bình luận {comment_data.get('id')} vì bài viết {post_id} không tồn tại")
                return

            # Chèn thêm hoặc cập nhật dữ liệu bình luận
            self.cur.execute("""
                INSERT INTO reddit_data.comments (
                    comment_id, post_id, parent_id, body, author, score,
                    created_utc, created_date, is_submitter, collected_utc
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (comment_id)
                DO UPDATE SET
                    score = EXCLUDED.score,
                    collected_utc = EXCLUDED.collected_utc;
            """,(
                comment_data.get('id'),
                comment_data.get('post_id'),
                comment_data.get('parent_id'),
                comment_data.get('body'),
                comment_data.get('author'),
                comment_data.get('score'),
                comment_data.get('created_utc'),
                comment_data.get('created_date'),
                comment_data.get('is_submitter', False),
                comment_data.get('collected_utc')
            ))

            # Cập nhật bảng user_activity
            self._update_user_activity(comment_data.get('author'), is_post=False)

            logger.debug(f"Đã lưu bình luận {comment_data.get('id')} vào PostgreSQL")

        except Exception as e:
            logger.error(f"Lỗi khi xử lý bình luận {comment_data.get('id')}: {str(e)}")
            raise

    def _ensure_subreddit_exists(self, subreddit_name):
        """
           Đảm bảo subreddit tồn tại trong bảng subreddits

           Args:
               subreddit_name (str): Tên subreddit
        """
        if not subreddit_name:
            return

        try:
            # Kiểm tra xem subreddit đã tồn tại hay chưa
            self.cur.execute(
                "SELECT COUNT(*) FROM reddit_data.subreddits WHERE name = %s",
                (subreddit_name,)
            )
            count = self.cur.fetchone()[0]

            # Nếu chưa tồn tại, thêm mới
            if count == 0:
                self.cur.execute(
                    "INSERT INTO reddit_data.subreddits (name) VALUES (%s) RETURNING subreddit_id",
                    (subreddit_name,)
                )
                self.conn.commit()
                logger.info(f"Đã thêm subreddit mới: {subreddit_name}")

        except Exception as e:
            logger.error(f"Lỗi khi kiểm tra / thêm subreddit {subreddit_name}: {str(e)}")
            raise

    def _update_user_activity(self, username, is_post=False):
        """
            Cập nhật hoặc thêm mới vào bảng user_activity

            Args:
                username (str): Tên người dùng
                is_post (bool): True nếu đây là bài viết, False nếu là bình luận
        """
        if not username or username == "[deleted]":
            return

        try:
            # Kiểm tra sự tồn tại của user
            self.cur.execute(
                "SELECT user_id, post_count, comment_count FROM reddit_data.user_activity WHERE username = %s",
                (username, )
            )
            result = self.cur.fetchone()

            current_time = time.strftime('%Y-%m-%d %H:%M:%S')

            # Nếu chưa tồn tại thì thêm mới
            if not result:
                self.cur.execute("""
                    INSERT INTO reddit_data.user_activity (
                        username, post_count, comment_count, first_seen, last_seen
                    ) VALUES (%s, %s, %s, %s, %s)
                """, (
                    username,
                    1 if is_post else 0,
                    0 if is_post else 1,
                    current_time,
                    current_time
                ))
            else:
                # Nếu đã tồn tại thì cập nhật
                user_id, post_count, comment_count = result
                self.cur.execute("""
                    UPDATE reddit_data.user_activity
                    SET post_count = %s,
                        comment_count = %s,
                        last_seen = %s
                    WHERE user_id = %s
                """, (
                    post_count + 1 if is_post else post_count,
                    comment_count + 1 if not is_post else comment_count,
                    current_time,
                    user_id
                ))
        except Exception as e:
            logger.error(f"Lỗi khi cập nhật hoạt động người dùng {username}: {str(e)}")

    def close(self):
        """Đóng kết nối PostgreSQL và Kafka consumer"""
        try:
            if hasattr(self, 'cur') and self.cur:
                self.cur.close()
            if hasattr(self, 'conn') and self.conn:
                self.conn.close()
            if hasattr(self, 'consumer') and self.consumer:
                self.consumer.close()
            logger.info("Đã đóng tất cả các kết nối")
        except Exception as e:
            logger.error(f"Lỗi khi đóng kết nối: {str(e)}")




