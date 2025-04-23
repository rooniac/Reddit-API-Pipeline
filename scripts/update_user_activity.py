# Đây chỉ là file phụ để chạy cập nhật activity trong quá trình phát triển -- WILL BE DELETED LATER

import sys
import os
import psycopg2

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
from src.utils.logger import setup_logger

logger = setup_logger("update_user_activity", "logs/update_user_activity.log")


def update_user_activity():
    """Cập nhật bảng user_activity dựa trên dữ liệu posts và comments"""
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

        logger.info("Cập nhật bảng user_activity")

        # Xóa dữ liệu cũ
        cur.execute("TRUNCATE TABLE reddit_data.user_activity RESTART IDENTITY")

        # Tạo bảng tạm thời để lưu thống kê bài viết
        cur.execute("""
            CREATE TEMP TABLE post_stats AS
            SELECT 
                p.author as username,
                COUNT(p.post_id) as post_count,
                AVG(p.score) as avg_post_score,
                MIN(p.created_date) as first_seen,
                MAX(p.created_date) as last_seen,
                ARRAY_AGG(DISTINCT s.name) as active_subreddits
            FROM 
                reddit_data.posts p
                JOIN reddit_data.subreddits s ON p.subreddit_id = s.subreddit_id
            WHERE 
                p.author IS NOT NULL AND p.author != '[deleted]'
            GROUP BY 
                p.author
        """)

        # Tạo bảng tạm thời để lưu thống kê bình luận
        cur.execute("""
            CREATE TEMP TABLE comment_stats AS
            SELECT 
                c.author as username,
                COUNT(c.comment_id) as comment_count,
                AVG(c.score) as avg_comment_score,
                MIN(c.created_date) as first_seen,
                MAX(c.created_date) as last_seen,
                ARRAY_AGG(DISTINCT s.name) as active_subreddits
            FROM 
                reddit_data.comments c
                JOIN reddit_data.posts p ON c.post_id = p.post_id
                JOIN reddit_data.subreddits s ON p.subreddit_id = s.subreddit_id
            WHERE 
                c.author IS NOT NULL AND c.author != '[deleted]'
            GROUP BY 
                c.author
        """)

        # Thêm người dùng từ bài viết
        cur.execute("""
            INSERT INTO reddit_data.user_activity (
                username, post_count, comment_count, avg_post_score, avg_comment_score, 
                first_seen, last_seen, active_subreddits
            )
            SELECT 
                ps.username,
                ps.post_count,
                0 as comment_count,
                ps.avg_post_score,
                0 as avg_comment_score,
                ps.first_seen,
                ps.last_seen,
                ps.active_subreddits
            FROM 
                post_stats ps
        """)

        # Cập nhật thông tin hoặc thêm người dùng từ bình luận
        cur.execute("""
            WITH user_update AS (
                SELECT 
                    cs.username,
                    cs.comment_count,
                    cs.avg_comment_score,
                    cs.first_seen,
                    cs.last_seen,
                    cs.active_subreddits,
                    ua.user_id
                FROM 
                    comment_stats cs
                    LEFT JOIN reddit_data.user_activity ua ON cs.username = ua.username
            )

            UPDATE reddit_data.user_activity ua
            SET 
                comment_count = uu.comment_count,
                avg_comment_score = uu.avg_comment_score,
                first_seen = CASE WHEN uu.first_seen < ua.first_seen THEN uu.first_seen ELSE ua.first_seen END,
                last_seen = CASE WHEN uu.last_seen > ua.last_seen THEN uu.last_seen ELSE ua.last_seen END,
                active_subreddits = (
                    SELECT ARRAY(
                        SELECT DISTINCT unnest(ua.active_subreddits || uu.active_subreddits)
                    )
                )
            FROM 
                user_update uu
            WHERE 
                ua.user_id = uu.user_id AND uu.user_id IS NOT NULL
        """)

        # Thêm người dùng mới (chỉ có bình luận, không có bài viết)
        cur.execute("""
            INSERT INTO reddit_data.user_activity (
                username, post_count, comment_count, avg_post_score, avg_comment_score, 
                first_seen, last_seen, active_subreddits
            )
            SELECT 
                cs.username,
                0 as post_count,
                cs.comment_count,
                0 as avg_post_score,
                cs.avg_comment_score,
                cs.first_seen,
                cs.last_seen,
                cs.active_subreddits
            FROM 
                comment_stats cs
            WHERE 
                NOT EXISTS (
                    SELECT 1 FROM reddit_data.user_activity ua WHERE ua.username = cs.username
                )
        """)

        # Lấy tổng số người dùng
        cur.execute("SELECT COUNT(*) FROM reddit_data.user_activity")
        total_user_count = cur.fetchone()[0]

        conn.commit()
        logger.info(f"Tổng số người dùng sau khi cập nhật: {total_user_count}")

    except Exception as e:
        logger.error(f"Lỗi khi cập nhật bảng user_activity: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            cur.close()
            conn.close()


if __name__ == "__main__":
    update_user_activity()