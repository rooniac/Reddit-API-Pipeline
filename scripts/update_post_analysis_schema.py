# scripts/update_post_analysis_schema.py
import sys
import os
import psycopg2

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
from src.utils.logger import setup_logger

logger = setup_logger("update_schema", "logs/update_schema.log")


def update_schema():
    """Cập nhật schema cho bảng post_analysis"""
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

        # Kiểm tra xem bảng post_analysis đã tồn tại chưa
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'reddit_data'
                AND table_name = 'post_analysis'
            )
        """)

        table_exists = cur.fetchone()[0]

        if not table_exists:
            # Tạo bảng post_analysis với ràng buộc UNIQUE cho post_id
            cur.execute("""
                CREATE TABLE reddit_data.post_analysis (
                    analysis_id SERIAL PRIMARY KEY,
                    post_id VARCHAR(10) REFERENCES reddit_data.posts(post_id),
                    sentiment_score FLOAT,
                    word_count INT,
                    unique_words INT,
                    tech_mentioned TEXT[],
                    companies_mentioned TEXT[],
                    skills_mentioned TEXT[],
                    topics TEXT[],
                    is_question BOOLEAN,
                    processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT unique_post_id UNIQUE (post_id)
                )
            """)
            logger.info("Đã tạo bảng post_analysis")
        else:
            # Kiểm tra xem ràng buộc UNIQUE đã tồn tại chưa
            cur.execute("""
                SELECT COUNT(*) FROM pg_constraint
                WHERE conrelid = 'reddit_data.post_analysis'::regclass
                AND contype = 'u'
                AND conname = 'unique_post_id'
            """)

            constraint_exists = cur.fetchone()[0] > 0

            if not constraint_exists:
                # Thêm ràng buộc UNIQUE nếu chưa tồn tại
                cur.execute("""
                    ALTER TABLE reddit_data.post_analysis
                    ADD CONSTRAINT unique_post_id UNIQUE (post_id)
                """)
                logger.info("Đã thêm ràng buộc UNIQUE cho cột post_id")

            # Kiểm tra và thêm các cột mới nếu cần
            cur.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                   WHERE table_schema = 'reddit_data' 
                                   AND table_name = 'post_analysis' 
                                   AND column_name = 'tech_mentioned') THEN
                        ALTER TABLE reddit_data.post_analysis ADD COLUMN tech_mentioned TEXT[];
                    END IF;

                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                   WHERE table_schema = 'reddit_data' 
                                   AND table_name = 'post_analysis' 
                                   AND column_name = 'skills_mentioned') THEN
                        ALTER TABLE reddit_data.post_analysis ADD COLUMN skills_mentioned TEXT[];
                    END IF;

                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                   WHERE table_schema = 'reddit_data' 
                                   AND table_name = 'post_analysis' 
                                   AND column_name = 'is_question') THEN
                        ALTER TABLE reddit_data.post_analysis ADD COLUMN is_question BOOLEAN;
                    END IF;

                    IF NOT EXISTS (SELECT 1 FROM information_schema.columns 
                                   WHERE table_schema = 'reddit_data' 
                                   AND table_name = 'post_analysis' 
                                   AND column_name = 'topics') THEN
                        ALTER TABLE reddit_data.post_analysis ADD COLUMN topics TEXT[];
                    END IF;
                END
                $$;
            """)
            logger.info("Đã cập nhật schema cho bảng post_analysis")

        # Kiểm tra xem bảng tech_trends đã tồn tại chưa
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'reddit_data'
                AND table_name = 'tech_trends'
            )
        """)

        trends_exists = cur.fetchone()[0]

        if not trends_exists:
            # Tạo bảng tech_trends
            cur.execute("""
                CREATE TABLE reddit_data.tech_trends (
                    trend_id SERIAL PRIMARY KEY,
                    tech_name VARCHAR(100),
                    mention_count INT,
                    week_start DATE,
                    sentiment_avg FLOAT,
                    subreddit_id INT REFERENCES reddit_data.subreddits(subreddit_id),
                    processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            logger.info("Đã tạo bảng tech_trends")

        conn.commit()

    except Exception as e:
        logger.error(f"Lỗi khi cập nhật schema: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()


if __name__ == "__main__":
    update_schema()