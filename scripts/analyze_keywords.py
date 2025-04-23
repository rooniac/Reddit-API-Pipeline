import os
import sys
import time

# Thêm thư mục gốc của dự án vào sys.path để có thể import các module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_analysis.keyword_analyzer import KeywordAnalyzer
from src.utils.logger import setup_logger

# Thiết lập logger
logger = setup_logger("keyword_analysis", "logs/keyword_analysis.log")


def main():
    """
       Hàm chính để phân tích từ khóa và chủ đề từ dữ liệu Reddit
    """
    logger.info("Bắt đầu quá trình phân tích từ khóa và chủ đề")
    start_time = time.time()

    analyzer = None
    try:
        analyzer = KeywordAnalyzer()

        # Phân tích các bài viết chưa được phân tích với đa luồng
        # Sử dụng cài đặt nâng cao với 8 luồng và batch_size 100
        post_count = analyzer.analyze_all_posts_parallel(max_workers=8, batch_size=100)
        logger.info(f"Đã phân tích {post_count} bài viết")

        # Cập nhật xu hướng công nghệ
        trend_count = analyzer.update_tech_trends()
        logger.info(f"Đã cập nhật {trend_count} xu hướng công nghệ")

    except Exception as e:
        logger.error(f"Lỗi trong quá trình phân tích từ khóa: {str(e)}")
    finally:
        if analyzer:
            analyzer.close()

    # Tính thời gian thực thi
    end_time = time.time()
    duration = end_time - start_time
    hours, remainder = divmod(duration, 3600)
    minutes, seconds = divmod(remainder, 60)

    logger.info(f"Hoàn thành quá trình phân tích từ khóa và chủ đề")
    logger.info(f"Thời gian thực thi: {int(hours)} giờ {int(minutes)} phút {int(seconds)} giây")


if __name__ == "__main__":
    main()