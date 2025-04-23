# scripts/update_comment_analysis_schema.py
import sys
import os
import psycopg2

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
from src.utils.logger import setup_logger

logger = setup_logger("update_schema", "logs/update_schema.log")


def update_schema():
    """Cập nhật schema cho bảng comment_analysis"""
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

        # Kiểm tra xem bảng comment_analysis đã tồn tại chưa
        cur.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_schema = 'reddit_data'
                AND table_name = 'comment_analysis'
            )
        """)

        table_exists = cur.fetchone()[0]

        if not table_exists:
            # Tạo bảng comment_analysis
            cur.execute("""
                CREATE TABLE reddit_data.comment_analysis (
                    analysis_id SERIAL PRIMARY KEY,
                    comment_id VARCHAR(10) REFERENCES reddit_data.comments(comment_id),
                    sentiment_score FLOAT,
                    word_count INT,
                    tech_mentioned TEXT[],
                    is_answer BOOLEAN,
                    processed_date TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    CONSTRAINT unique_comment_id UNIQUE (comment_id)
                )
            """)
            logger.info("Đã tạo bảng comment_analysis")

            # Tạo indexes để tối ưu truy vấn
            cur.execute("""
                CREATE INDEX idx_comment_analysis_sentiment ON reddit_data.comment_analysis(sentiment_score);
                CREATE INDEX idx_comment_analysis_comment_id ON reddit_data.comment_analysis(comment_id);
            """)
            logger.info("Đã tạo indexes cho bảng comment_analysis")

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