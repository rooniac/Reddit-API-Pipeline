import os
import json
import psycopg2
import sys
from datetime import datetime

# Thêm thư mục gốc của dự án vào sys.path để có thể import các module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
from src.utils.logger import setup_logger

logger = setup_logger("load_raw_to_db", "logs/load_raw_to_db.log")

def load_data_to_db():
    """Load dữ liệu từ files JSON vào database"""
    conn = None
    try:
        # Kết nối PostgreSQL
        conn = psycopg2.connect(
            host=POSTGRES_HOST,
            port=POSTGRES_PORT,
            dbname=POSTGRES_DB,
            user=POSTGRES_USER,
            password=POSTGRES_PASSWORD
        )
        cur = conn.cursor()

        # Load posts trước để tránh lỗi comment không tìm được post
        load_posts(cur, conn)

        # Sau đó load comments
        load_comments(cur, conn)

        # Cập nhật bảng user_activity
        update_user_activity(cur, conn)

        logger.info("Hoàn thành việc load dữ liệu raw vào database (Luồng phụ)")

    except Exception as e:
        logger.error(f"Lỗi tổng thể: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

def load_posts(cur, conn):
    """Load dữ liệu bài viết từ files JSON vào database"""
    posts_dir = "data/raw/posts"
    if not os.path.exists(posts_dir):
        logger.warning(f"Thư mục {posts_dir} không tồn tại")
        return

    posts_files = [f for f in os.listdir(posts_dir) if f.endswith('json')]

    logger.info(f"Tìm thấy {len(posts_files)} files JSON bài viết")

    # Đếm các bài viết
    total_files = len(posts_files)
    processed = 0
    inserted = 0
    updated = 0
    errors = 0
    skipped = 0

    # Đảm bảo subreddits tồn tại
    subreddits = set()
    for file_name in posts_files:
        try:
            with open(os.path.join(posts_dir, file_name), 'r', encoding='utf-8') as f:
                post_data = json.load(f)
                if 'subreddit' in post_data and post_data['subreddit']:
                    subreddits.add(post_data['subreddit'])
        except:
            pass

    # Tạo subreddits trong database
    for subreddit in subreddits:
        try:
            cur.execute(
                "INSERT INTO reddit_data.subreddits (name) VALUES (%s) ON CONFLICT (name) DO NOTHING",
                (subreddit,)
            )
        except Exception as e:
            logger.error(f"Lỗi khi thêm subreddit {subreddit}: {str(e)}")

    conn.commit()
    logger.info(f"Đã thêm {len(subreddits)} subreddits vào database")

    # Xử lý từng file bài viết
    for file_name in posts_files:
        processed += 1
        if processed % 100 == 0:
            logger.info(f"Đã xử lý {processed}/{total_files} files bài viết")

        try:
            with open(os.path.join(posts_dir, file_name), 'r', encoding='utf-8') as f:
                post_data = json.load(f)

            # Kiểm tra dữ liệu cần thiết
            if 'id' not in post_data or not post_data['id'] or 'subreddit' not in post_data or not post_data['subreddit']:
                logger.warning(f"File {file_name} thiếu thông tin cần thiết, bỏ qua")
                skipped += 1
                continue

            # Lấy subreddit_id
            cur.execute(
                "SELECT subreddit_id FROM reddit_data.subreddits WHERE name = %s",
                (post_data.get('subreddit'),)
            )
            result = cur.fetchone()
            if not result:
                logger.warning(f"Không tìm thấy subreddit {post_data.get('subreddit')}, bỏ qua bài viết")
                skipped += 1
                continue

            subreddit_id = result[0]

            # Kiểm tra xem bài viết đã tồn tại chưa
            cur.execute(
                "SELECT post_id FROM reddit_data.posts WHERE post_id = %s",
                (post_data.get('id'),)
            )
            exists = cur.fetchone() is not None

            # Chèn hoặc cập nhật bài viết
            try:
                if exists:
                    cur.execute("""
                        UPDATE reddit_data.posts SET
                            subreddit_id = %s,
                            title = %s,
                            text = %s,
                            url = %s,
                            author = %s,
                            score = %s,
                            upvote_ratio = %s,
                            num_comments = %s,
                            created_utc = %s,
                            created_date = %s,
                            is_self = %s,
                            is_video = %s,
                            over_18 = %s,
                            permalink = %s,
                            link_flair_text = %s,
                            collected_utc = %s
                        WHERE post_id = %s
                    """, (
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
                        post_data.get('collected_utc'),
                        post_data.get('id')
                    ))
                    updated += 1
                else:
                    cur.execute("""
                        INSERT INTO reddit_data.posts (
                            post_id, subreddit_id, title, text, url, author, score, upvote_ratio,
                            num_comments, created_utc, created_date, is_self, is_video,
                            over_18, permalink, link_flair_text, collected_utc
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                    inserted += 1

                    # Commit sau mỗi 100 bài viết để tránh transaction quá lớn
                if (inserted + updated) % 100 == 0:
                    conn.commit()

            except Exception as e:
                logger.error(f"Lỗi khi chèn/cập nhật bài viết từ file {file_name}: {str(e)}")
                errors += 1
        except Exception as e:
            logger.error(f"Lỗi khi xử lý file {file_name}: {str(e)}")
            errors += 1
    # Commit sau khi thay đổi
    conn.commit()

    logger.info(f"Kết quả xử lý bài viết: Đã xử lý {processed} files, chèn mới {inserted}, cập nhật {updated}, bỏ qua {skipped}, lỗi {errors}")


def load_comments(cur, conn):
    """Load dữ liệu bình luận từ files JSON vào database"""
    # Đọc thư mục dữ liệu thô
    comments_dir = "data/raw/comments"
    if not os.path.exists(comments_dir):
        logger.warning(f"Thư mục {comments_dir} không tồn tại")
        return

    comment_files = [f for f in os.listdir(comments_dir) if f.endswith('.json')]

    logger.info(f"Tìm thấy {len(comment_files)} files JSON bình luận")

    # Đếm các bình luận
    total_files = len(comment_files)
    processed = 0
    inserted = 0
    updated = 0
    errors = 0
    skipped = 0

    # Xử lý từng file bình luận
    for file_name in comment_files:
        processed += 1
        if processed % 100 == 0:
            logger.info(f"Đã xử lý {processed}/{total_files} files bình luận")

        try:
            with open(os.path.join(comments_dir, file_name), 'r', encoding='utf-8') as f:
                comment_data = json.load(f)

            # Kiểm tra dữ liệu cần thiết
            if 'id' not in comment_data or not comment_data['id'] or 'post_id' not in comment_data or not comment_data[
                'post_id']:
                logger.warning(f"File {file_name} thiếu thông tin cần thiết, bỏ qua")
                skipped += 1
                continue

            # Kiểm tra xem bài viết tương ứng có tồn tại không
            cur.execute(
                "SELECT post_id FROM reddit_data.posts WHERE post_id = %s",
                (comment_data.get('post_id'),)
            )
            if cur.fetchone() is None:
                logger.warning(
                    f"Bỏ qua bình luận {comment_data.get('id')} vì bài viết {comment_data.get('post_id')} không tồn tại")
                skipped += 1
                continue

            # Kiểm tra xem bình luận đã tồn tại chưa
            cur.execute(
                "SELECT comment_id FROM reddit_data.comments WHERE comment_id = %s",
                (comment_data.get('id'),)
            )
            exists = cur.fetchone() is not None

            # Chèn hoặc cập nhật bình luận
            try:
                if exists:
                    cur.execute("""
                        UPDATE reddit_data.comments SET
                            post_id = %s,
                            parent_id = %s,
                            body = %s,
                            author = %s,
                            score = %s,
                            created_utc = %s,
                            created_date = %s,
                            is_submitter = %s,
                            collected_utc = %s
                        WHERE comment_id = %s
                    """, (
                        comment_data.get('post_id'),
                        comment_data.get('parent_id'),
                        comment_data.get('body'),
                        comment_data.get('author'),
                        comment_data.get('score'),
                        comment_data.get('created_utc'),
                        comment_data.get('created_date'),
                        comment_data.get('is_submitter', False),
                        comment_data.get('collected_utc'),
                        comment_data.get('id')
                    ))
                    updated += 1
                else:
                    cur.execute("""
                        INSERT INTO reddit_data.comments (
                            comment_id, post_id, parent_id, body, author, score,
                            created_utc, created_date, is_submitter, collected_utc
                        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                    """, (
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
                    inserted += 1

                # Commit sau mỗi 100 bình luận để tránh transaction quá lớn
                if (inserted + updated) % 100 == 0:
                    conn.commit()

            except Exception as e:
                logger.error(f"Lỗi khi chèn/cập nhật bình luận từ file {file_name}: {str(e)}")
                errors += 1

        except Exception as e:
            logger.error(f"Lỗi khi xử lý file {file_name}: {str(e)}")
            errors += 1

    # Commit các thay đổi còn lại
    conn.commit()

    logger.info(f"Kết quả xử lý bình luận: Đã xử lý {processed} files, chèn mới {inserted}, cập nhật {updated}, bỏ qua {skipped}, lỗi {errors}")


def update_user_activity(cur, conn):
    """Cập nhật bảng user_activity dựa trên dữ liệu posts và comments"""
    logger.info("Cập nhật bảng user_activity")

    try:
        # Xóa dữ liệu cũ
        cur.execute("TRUNCATE TABLE reddit_data.user_activity RESTART IDENTITY")

        # Chèn dữ liệu từ posts và comments
        cur.execute("""
            WITH user_stats AS (
                SELECT 
                    author as username,
                    COUNT(DISTINCT CASE WHEN source = 'post' THEN id END) as post_count,
                    COUNT(DISTINCT CASE WHEN source = 'comment' THEN id END) as comment_count,
                    AVG(CASE WHEN source = 'post' THEN score END) as avg_post_score,
                    AVG(CASE WHEN source = 'comment' THEN score END) as avg_comment_score,
                    MIN(created_date) as first_seen,
                    MAX(created_date) as last_seen,
                    ARRAY_AGG(DISTINCT subreddit) as active_subreddits
                FROM (
                    SELECT 
                        post_id as id, 
                        author, 
                        score, 
                        created_date, 
                        s.name as subreddit, 
                        'post' as source
                    FROM reddit_data.posts p
                    JOIN reddit_data.subreddits s ON p.subreddit_id = s.subreddit_id

                    UNION ALL

                    SELECT 
                        comment_id as id, 
                        author, 
                        score, 
                        created_date, 
                        s.name as subreddit, 
                        'comment' as source
                    FROM reddit_data.comments c
                    JOIN reddit_data.posts p ON c.post_id = p.post_id
                    JOIN reddit_data.subreddits s ON p.subreddit_id = s.subreddit_id
                ) combined
                WHERE author IS NOT NULL AND author != '[deleted]'
                GROUP BY author
            )
            INSERT INTO reddit_data.user_activity (
                username, post_count, comment_count, avg_post_score, 
                avg_comment_score, first_seen, last_seen, active_subreddits
            )
            SELECT 
                username, post_count, comment_count, 
                COALESCE(avg_post_score, 0) as avg_post_score,
                COALESCE(avg_comment_score, 0) as avg_comment_score,
                first_seen, last_seen, active_subreddits
            FROM user_stats
        """)

        # Lấy số lượng người dùng đã cập nhật
        cur.execute("SELECT COUNT(*) FROM reddit_data.user_activity")
        user_count = cur.fetchone()[0]

        conn.commit()
        logger.info(f"Đã cập nhật thông tin cho {user_count} người dùng")

    except Exception as e:
        logger.error(f"Lỗi khi cập nhật bảng user_activity: {str(e)}")
        conn.rollback()

if __name__ == "__main__":
    load_data_to_db()








