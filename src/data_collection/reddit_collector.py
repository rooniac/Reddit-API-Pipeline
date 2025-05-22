import json
import time
from datetime import datetime
import praw
from praw import models
import os
from kafka import KafkaProducer

from src.utils.config import (
    REDDIT_CLIENT_ID, REDDIT_CLIENT_SECRET, REDDIT_USER_AGENT,
    REDDIT_USERNAME, REDDIT_PASSWORD, KAFKA_BOOTSTRAP_SERVERS,
    KAFKA_POSTS_TOPIC, KAFKA_COMMENTS_TOPIC, DEFAULT_POST_LIMIT
)
from src.utils.logger import setup_logger

logger = setup_logger(__name__, "logs/reddit_collector.log")

class RedditCollector:
    """
        Class chịu trách nhiệm thu thập dữ liệu từ Reddit API và gửi tới Kafka
    """

    def __init__(self, subreddits=None, post_limit=DEFAULT_POST_LIMIT):
        """
            Khởi tạo Reddit API client và Kafka producer

            Args:
                subreddits (list): Danh sách các subreddit cần thu thập dữ liệu
                post_limit (int): Số lượng bài viết tối đa cần thu thập cho mỗi subreddit
        """
        if subreddits is None:
            self.subreddits = ["dataengineering"]
        else:
            self.subreddits = subreddits

        self.post_limit = post_limit
        self.total_posts_collected = 0
        self.total_comments_collected = 0

        # Create Reddit API client
        logger.info("Khoi tao Reddit API client")
        try:
            self.reddit = praw.Reddit(
                client_id = REDDIT_CLIENT_ID,
                client_secret = REDDIT_CLIENT_SECRET,
                user_agent = REDDIT_USER_AGENT,
                username = REDDIT_USERNAME,
                password = REDDIT_PASSWORD
            )
            logger.info("Đã kết nối thành công với Reddit API")
        except Exception as e:
            logger.error(f"Lỗi khi kết nối vưới Reddit API: {str(e)}")
            raise

        # Create Kafka producer
        logger.info(f"Khởi tạo Kafka producer, kết nối với {KAFKA_BOOTSTRAP_SERVERS}")
        try:
            self.producer = KafkaProducer(
                bootstrap_servers = KAFKA_BOOTSTRAP_SERVERS,
                value_serializer = lambda v: json.dumps(v).encode('utf-8'),
                key_serializer = lambda k: k.encode('utf-8') if k else None
            )
            logger.info("Đã kết nối thành công với Kafka")
        except Exception as e:
            logger.error(f"Lỗi khi kết nối với Kafka: {str(e)}")
            raise


    def collect_posts_with_pagination(self, subreddit_name, sort_by='hot', max_posts=None):
        """
            Thu thập bài viết từ một subreddit cụ thể sử dụng phương pháp phân trang để lấy nhiều hơn

            Args:
                subreddit_name (str): Tên của subreddit
                sort_by (str): Phương thức sắp xếp ('hot', 'new', 'top', 'rising')
                max_posts (int): Số lượng bài viết tối đa cần thu thập, None để lấy tất cả

            Returns:
                int: Số lượng bài viết đã thu thập
        """

        logger.info(f"Bắt đầu thu thập bài viết từ r/{subreddit_name} ({sort_by})")

        subreddit = self.reddit.subreddit(subreddit_name)

        count = 0
        last_id = None
        batch_size = 100 # Số lượng bài viết lấy trong mỗi lần call API

        # Lặp để thu thập dữ liệu theo từng trang
        while True:
            try:
                # Áp dụng phương thức sắp xếp và pagination
                params = {"after": last_id} if last_id else {}

                if sort_by == "hot":
                    posts_batch = list(subreddit.hot(limit=batch_size, params=params))
                elif sort_by == "new":
                    posts_batch = list(subreddit.new(limit=batch_size, params=params))
                elif sort_by == "top":
                    posts_batch = list(subreddit.top(limit=batch_size, params=params))
                elif sort_by == "rising":
                    posts_batch = list(subreddit.rising(limit=batch_size, params=params))
                else:
                    logger.warning(f"Phương thức sắp xếp không hợp lệ: {sort_by}, tiến hành sử dụng hot thay thế")
                    posts_batch = list(subreddit.hot(limit=batch_size, params=params))

                    # Nếu không có thêm bài viết nào, thoát vòng lặp
                if not posts_batch:
                    logger.info(f"Không còn bài viết nào để thu thập từ r/{subreddit_name} ({sort_by})")
                    break

                # Xử lý mỗi bài viết trong batch
                for post in posts_batch:
                    if post.stickied:
                        continue

                    if max_posts and count >= max_posts:
                        logger.info(f"Đã đạt giới hạn {max_posts} bài viết từ r/{subreddit_name} ({sort_by})")
                        return count

                    # Xử lý bài viết
                    try:
                        post_data = {
                            "id": post.id,
                            "title": post.title,
                            "text": post.selftext,
                            "url": post.url,
                            "author": post.author.name if post.author else "[deleted]",
                            "score": post.score,
                            "upvote_ratio": post.upvote_ratio,
                            "num_comments": post.num_comments,
                            "created_utc": post.created_utc,
                            "created_date": datetime.fromtimestamp(post.created_utc).strftime('%Y-%m-%d %H:%M:%S'),
                            "subreddit": post.subreddit.display_name,
                            "permalink": post.permalink,
                            "is_self": post.is_self,
                            "is_video": post.is_video if hasattr(post, 'is_video') else False,
                            "over_18": post.over_18,
                            "spoiler": post.spoiler if hasattr(post, 'spoiler') else False,
                            "link_flair_text": post.link_flair_text,
                            "sort_type": sort_by,
                            "collected_utc": int(time.time())
                        }

                        # Gửi dữ liệu tới Kafka
                        self.producer.send(KAFKA_POSTS_TOPIC, key=post.id, value=post_data)

                        # Lưu trữ dữ liệu vào file JSON
                        self._save_to_json(post_data, f"data/raw/posts/{subreddit_name}_{post.id}.json")

                        count += 1
                        self.total_posts_collected += 1

                        if count % 10 == 0:
                            logger.info(f"Tiến độ: Đã thu thập {count} bài viết từ r/{subreddit_name} ({sort_by})")

                        # Thu thập bình luận cho bài viết hiện tại
                        comments_count = self.collect_comments(post)

                        # Rate limiting
                        time.sleep(1)

                    except Exception as e:
                        logger.error(f"Lỗi khi xử lý bài viết {post.id}: {str(e)}")

                # Lưu ID của bài viết cuối cùng để tiếp tục phân trang
                if posts_batch:
                    last_id = f"t3_{posts_batch[-1].id}"
                    logger.debug(f"Sử dụng last_id: {last_id} cho trang tiếp theo")

                    # Rate limiting
                    time.sleep(2)

            except Exception as e:
                logger.error(f"Lỗi trong quá trình thu thập từ r/{subreddit_name} ({sort_by}): {str(e)}")
                # Nếu gặp lỗi, đợi lâu hơn trước khi thử lại
                time.sleep(10)
                break

        logger.info(f"Đã hoàn thành thu thập {count} bài viết từ r/{subreddit_name} ({sort_by})")
        return count

    def collect_comments(self, post):
        """
            Thu thập bình luận từ một bài viết

            Args:
                post (praw.models.Submission): Đối tượng bài viết

            Returns:
                int: Số lượng bình luận đã thu thập
        """
        logger.info(f"Thu thập bình luận cho bài viết ID: {post.id}, tiêu đề: {post.title[:30]}...")

        # Đảm bảo tất cả bình luận được tải (thay thế MoreComments object)
        try:
            # Thiết lập giới hạn cho việc thu thập bình luận để tránh quá tải hệ thống
            post.comments.replace_more(limit=None)  # Sử dụng None để lấy tất cả bình luận
            count = 0

            # Đệ quy duyệt qua từng bình luận và bình luận con của bình luận
            def process_comment(comment_obj, parent_id=None):
                nonlocal count

                try:
                    comment_data = {
                        "id": comment_obj.id,
                        "post_id": post.id,
                        "parent_id": parent_id if parent_id else comment_obj.parent_id,
                        "body": comment_obj.body,
                        "author": comment_obj.author.name if comment_obj.author else "[deleted]",
                        "score": comment_obj.score,
                        "created_utc": comment_obj.created_utc,
                        "created_date": datetime.fromtimestamp(comment_obj.created_utc).strftime('%Y-%m-%d %H:%M:%S'),
                        "is_submitter": comment_obj.is_submitter,
                        "subreddit": post.subreddit.display_name,
                        "collected_utc": int(time.time())
                    }

                    # Gửi data tới Kafka
                    self.producer.send(KAFKA_COMMENTS_TOPIC, key=comment_obj.id, value=comment_data)

                    # Lưu dữ liệu vào file JSON
                    self._save_to_json(comment_data,
                                       f"data/raw/comments/{post.subreddit.display_name}_{comment_obj.id}.json")

                    count += 1
                    self.total_comments_collected += 1

                    # Hiển thị thông tin tiến độ sau mỗi 100 bình luận
                    if count % 100 == 0:
                        logger.info(f"Tiến độ: Đã thu thập {count} bình luận cho bài viết ID: {post.id}")

                    # Xử lý các bình luận con
                    for reply in comment_obj.replies:
                        process_comment(reply, comment_obj.id)

                except Exception as e:
                    logger.error(f"Lỗi khi xử lý bình luận {comment_obj.id}: {str(e)}")
                    logger.debug(
                        f"Chi tiết bình luận: id={comment_obj.id}, author={comment_obj.author if comment_obj.author else '[deleted]'}")

            # Xử lý tất cả bình luận gốc trong bài viết
            for comment in post.comments:
                if isinstance(comment, praw.models.MoreComments):
                    continue
                process_comment(comment)

            logger.info(f"Đã thu thập {count} bình luận cho bài viết ID: {post.id}")
            return count
        except Exception as e:
            logger.error(f"Lỗi khi thu thập bình luận cho bài viết {post.id}: {str(e)}")
            return 0

    def collect_all_data(self, max_posts_per_type=None):
        """
            Thu thập dữ liệu từ tất cả subreddits được cấu hình

            Args:
                max_posts_per_type (int): Số lượng bài viết tối đa cần thu thập cho mỗi loại sắp xếp,
                                         None để lấy tất cả có thể
        """
        logger.info(f"Bắt đầu thu thập dữ liệu từ {len(self.subreddits)} subreddits: {', '.join(self.subreddits)}")
        start_time = time.time()

        # Đặt lại bộ đếm tổng
        self.total_posts_collected = 0
        self.total_comments_collected = 0

        for subreddit in self.subreddits:
            logger.info(f"Bắt đầu thu thập dữ liệu từ r/{subreddit}")

            try:
                # Thu thập các bài viết theo các cách sắp xếp khác nhau
                for sort_type in ["hot", "new", "top", "rising"]:
                    posts_count = self.collect_posts_with_pagination(subreddit, sort_by=sort_type,
                                                                     max_posts=max_posts_per_type)
                    logger.info(f"Đã thu thập {posts_count} bài viết từ r/{subreddit} ({sort_type})")

                    # Rate limiting
                    time.sleep(3)

                logger.info(f"Đã hoàn thành thu thập dữ liệu từ r/{subreddit}")

            except Exception as e:
                logger.error(f"Lỗi khi thu thập dữ liệu từ r/{subreddit}: {str(e)}")

            # Rate limiting
            time.sleep(5)

        end_time = time.time()
        duration = end_time - start_time
        hours, remainder = divmod(duration, 3600)
        minutes, seconds = divmod(remainder, 60)

        logger.info(f"Đã hoàn thành thu thập dữ liệu từ tất cả subreddits")
        logger.info(f"Tổng cộng: {self.total_posts_collected} bài viết và {self.total_comments_collected} bình luận")
        logger.info(f"Thời gian thu thập: {int(hours)} giờ, {int(minutes)} phút và {int(seconds)} giây")

    def _save_to_json(self, data, file_path):
        """
            Lưu trữ dữ liệu vào file JSON

            Args:
                data (dict): Dữ liệu cần lưu
                file_path (str): Đường dẫn tới file
        """

        # Tạo folder nếu chưa tồn tại
        os.makedirs(os.path.dirname(file_path), exist_ok=True)

        try:
            with open(file_path, 'w', encoding='utf-8') as f:
                json.dump(data, f, ensure_ascii=False, indent=2)
        except Exception as e:
            logger.error(f"Lỗi khi lưu dữ liệu vào {file_path}: {str(e)}")

    def close(self):
        """ Đóng kafka producer """
        if hasattr(self, 'producer') and self.producer:
            self.producer.flush()
            self.producer.close()
            logger.info("Kafka producer đã đóng")