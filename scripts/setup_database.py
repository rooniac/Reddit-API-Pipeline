import sys
import os
import psycopg2
from psycopg2 import sql

# Thêm thư mục gốc của dự án vào sys.path để có thể import các module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.utils.config import POSTGRES_HOST, POSTGRES_PORT, POSTGRES_DB, POSTGRES_USER, POSTGRES_PASSWORD
from src.utils.logger import setup_logger

# Thiết lập logger
logger = setup_logger("database_setup", "logs/database_setup.log")

def setup_database():
    """
        Thiết lập cơ sở dữ liệu bằng cách thực thi script SQL
    """

    logger.info("Bắt đầu thiết lập cơ sở dữ liệu PostgreSQL")

    conn = None
    try:
        conn = psycopg2.connect(
            host = POSTGRES_HOST,
            port = POSTGRES_PORT,
            dbname = POSTGRES_DB,
            user = POSTGRES_USER,
            password = POSTGRES_PASSWORD
        )

        # Tạo cursor
        cur = conn.cursor()

        # Đọc và thực thi script sql đã tạo sẵn
        with open('scripts/create_database_schema.sql', 'r', encoding='utf-8') as f:
            sql_script = f.read()

        cur.execute(sql_script)  # Thực thi script
        conn.commit()

        cur.close()
        logger.info("Đã thiết lập cơ sở dữ liệu thành công")

    except Exception as e:
        logger.error(f"Lỗi khi thiết lập cơ sở dữ liệu: {str(e)}")
        if conn:
            conn.rollback()
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    setup_database()