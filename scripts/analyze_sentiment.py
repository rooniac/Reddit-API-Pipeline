import sys
import os
import time
import argparse

# Thêm thư mục gốc của dự án vào sys.path để có thể import các module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_analysis.sentiment_analyzer import SentimentAnalyzer
from src.utils.logger import setup_logger

# Thiết lập logger
logger = setup_logger("sentiment_analysis", "logs/sentiment_analysis.log")


def main():
    """Hàm chính để phân tích tình cảm từ dữ liệu Reddit"""
    # Xử lý tham số dòng lệnh
    parser = argparse.ArgumentParser(description='Phân tích tình cảm từ dữ liệu Reddit')
    parser.add_argument('--max-workers', type=int, default=4, help='Số lượng worker threads tối đa')
    parser.add_argument('--batch-size', type=int, default=50, help='Kích thước của mỗi batch')
    parser.add_argument('--limit', type=int, help='Giới hạn số lượng bài viết phân tích')
    parser.add_argument('--analyze-comments', action='store_true', help='Phân tích bình luận')
    args = parser.parse_args()

    logger.info("Bắt đầu quá trình phân tích tình cảm")
    start_time = time.time()

    analyzer = None
    try:
        # Khởi tạo SentimentAnalyzer với connection pool
        analyzer = SentimentAnalyzer(min_conn=3, max_conn=10)

        # Phân tích tình cảm cho bài viết với đa luồng
        logger.info(f"Phân tích bài viết với {args.max_workers} worker threads và batch size {args.batch_size}")
        post_count = analyzer.analyze_all_posts_parallel(
            max_workers=args.max_workers,
            batch_size=args.batch_size,
            limit=args.limit
        )
        logger.info(f"Đã phân tích tình cảm cho {post_count} bài viết")

        # Phân tích bình luận nếu được yêu cầu
        if args.analyze_comments:
            comment_count = analyzer.analyze_all_comments_parallel(
                max_workers=args.max_workers,
                batch_size=args.batch_size * 2,  # Bình luận thường ngắn hơn nên batch lớn hơn
                limit=args.limit
            )
            logger.info(f"Đã phân tích tình cảm cho {comment_count} bình luận")

        # Cập nhật tình cảm cho các công nghệ
        tech_count = analyzer.update_tech_sentiment()
        logger.info(f"Đã cập nhật tình cảm cho {tech_count} công nghệ")

        # Phân tích xu hướng cảm xúc
        trends = analyzer.analyze_sentiment_trends(period_days=30, min_mentions=5)
        logger.info(f"Đã phân tích xu hướng cảm xúc cho {len(trends)} công nghệ")

    except Exception as e:
        logger.error(f"Lỗi trong quá trình phân tích tình cảm: {str(e)}")
    finally:
        # Đảm bảo đóng kết nối
        if analyzer:
            analyzer.close()

    # Tính thời gian thực thi
    end_time = time.time()
    duration = end_time - start_time
    hours, remainder = divmod(duration, 3600)
    minutes, seconds = divmod(remainder, 60)

    logger.info(f"Hoàn thành quá trình phân tích tình cảm")
    logger.info(f"Thời gian thực thi: {int(hours)} giờ {int(minutes)} phút {int(seconds)} giây")


if __name__ == "__main__":
    main()