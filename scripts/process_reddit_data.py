import sys
import os

# Thêm thư mục gốc của dự án vào sys.path để có thể import các module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_processing.kafka_consumer import RedditDataConsumer
from src.utils.logger import setup_logger

# Thiết lập logger
logger = setup_logger("reddit_data_processing", "logs/data_processing.log")

def main():
    """ Hàm xử lý dữ liệu từ kafka """
    logger.info("Bắt đầu quá trình xử lý dữ liệu Reddit từ Kafka")

    consumer = None
    try:
        # Khởi tạo consumer
        consumer = RedditDataConsumer()
        # Tiến hành xử lý dữ liệu
        consumer.process_data()

    except Exception as e:
        logger.error(f"Lỗi trong quá trình xử lý dữ liệu: {str(e)}")
    finally:
        # Đảm bảo các kết nối được đóng
        if consumer:
            consumer.close()

    logger.info("Hoàn thành quá trình xử lý dữ liệu")

if __name__ == "__main__":
    main()
















