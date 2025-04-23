import sys
import os

# Thêm thư mục gốc của dự án vào sys.path để có thể import các module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_collection.reddit_collector import RedditCollector
from src.utils.config import SUBREDDITS
from src.utils.logger import setup_logger

# Thiết lập logger
logger = setup_logger("reddit_data_collection", "logs/data_collection.log")

def main():
    """ Hàm chính thu thập dữ liệu từ reddit """
    logger.info("Starting Reddit data collection process")

    # Tạo thư mục lưu trữ dữ liệu nếu chưa tồn tại
    os.makedirs("data/raw/posts", exist_ok=True)
    os.makedirs("data/raw/comments", exist_ok=True)

    collector = None

    try:
        # Khởi tạo collector
        collector = RedditCollector(subreddits=SUBREDDITS)

        # Thu thập dữ liệu
        collector.collect_all_data(max_posts_per_type=500) # 500 bài viết mỗi loại sắp xếp

    except Exception as e:
        logger.error(f"Error during data collection: {str(e)}")
    finally:
        # Đảm bảo đóng kafka producer
        if collector:
            collector.close()

    logger.info("Reddit data collection process completed")

if __name__ == "__main__":
    main()