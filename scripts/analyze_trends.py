# scripts/analyze_trends.py
import sys
import os
import time
import argparse
from datetime import datetime

import pandas as pd

# Thêm thư mục gốc của dự án vào sys.path để có thể import các module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_analysis.trend_analyzer import TrendAnalyzer
from src.utils.logger import setup_logger

# Thiết lập logger
logger = setup_logger("trend_analysis", "logs/trend_analysis.log")


def main():
    """
    Hàm chính để phân tích xu hướng từ dữ liệu Reddit
    """
    # Xử lý tham số dòng lệnh
    parser = argparse.ArgumentParser(description='Phân tích xu hướng từ dữ liệu Reddit')
    parser.add_argument('--max-workers', type=int, default=4, help='Số lượng worker threads tối đa cho xử lý song song')
    parser.add_argument('--parallel', action='store_true', help='Chạy các phân tích song song')
    parser.add_argument('--min-mentions', type=int, default=5, help='Số lần đề cập tối thiểu cho các phân tích')
    parser.add_argument('--growth-threshold', type=int, default=50, help='Ngưỡng tăng trưởng % cho công nghệ mới nổi')
    parser.add_argument('--specific-analysis', choices=[
        'weekly_trends', 'tech_growth', 'emerging_tech', 'tech_correlation',
        'skill_demand', 'subreddit_trends', 'sentiment_popularity', 'time_trends', 'all'
    ], default='all', help='Chỉ chạy phân tích cụ thể')
    parser.add_argument('--time-unit', choices=['day', 'week', 'month', 'quarter'], default='week',
                        help='Đơn vị thời gian cho phân tích xu hướng theo thời gian')

    args = parser.parse_args()

    logger.info("Bắt đầu quá trình phân tích xu hướng")
    start_time = time.time()

    analyzer = None
    try:
        # Khởi tạo analyzer với connection pool
        analyzer = TrendAnalyzer(min_conn=3, max_conn=10)

        if args.specific_analysis == 'all':
            # Chạy tất cả các phân tích tự động
            results = analyzer.run_all_analyses(
                parallel=args.parallel,
                max_workers=args.max_workers
            )

            # In tóm tắt kết quả
            for analysis_name, result in results.items():
                if result is not None:
                    if isinstance(result, pd.DataFrame):
                        count = len(result)
                    elif isinstance(result, int):
                        # Nếu kết quả là số nguyên, sử dụng trực tiếp
                        count = result
                    elif hasattr(result, '__len__'):
                        # Nếu kết quả có phương thức __len__, sử dụng len()
                        count = len(result)
                    else:
                        # Trường hợp khác, chỉ báo là có kết quả
                        count = "có kết quả"
                    logger.info(f"Phân tích {analysis_name}: {count}")
                else:
                    logger.warning(f"Phân tích {analysis_name} không thành công")
        else:
            # Chạy phân tích cụ thể
            if args.specific_analysis == 'weekly_trends':
                trend_count = analyzer.analyze_weekly_tech_trends()
                logger.info(f"Đã phân tích {trend_count} xu hướng công nghệ theo tuần")

            elif args.specific_analysis == 'tech_growth':
                growth_df = analyzer.analyze_tech_growth(period_weeks=4)
                logger.info(f"Đã phân tích tăng trưởng cho {len(growth_df)} công nghệ")

            elif args.specific_analysis == 'emerging_tech':
                emerging_df = analyzer.analyze_emerging_technologies(
                    min_mentions=args.min_mentions,
                    growth_threshold=args.growth_threshold
                )
                logger.info(f"Đã xác định {len(emerging_df)} công nghệ mới nổi")

            elif args.specific_analysis == 'tech_correlation':
                correlation_matrix = analyzer.analyze_tech_correlation(min_mentions=args.min_mentions)
                logger.info(
                    f"Đã phân tích tương quan giữa {len(correlation_matrix) if hasattr(correlation_matrix, '__len__') else 0} công nghệ")

            elif args.specific_analysis == 'skill_demand':
                skill_trends = analyzer.analyze_skill_demand_trends()
                if not skill_trends.empty:
                    logger.info(f"Đã phân tích xu hướng nhu cầu kỹ năng qua {skill_trends['month'].nunique()} tháng")
                else:
                    logger.info("Không có dữ liệu về nhu cầu kỹ năng")

            elif args.specific_analysis == 'subreddit_trends':
                subreddit_trends = analyzer.analyze_subreddit_trends(min_mentions=args.min_mentions)
                if not subreddit_trends.empty:
                    logger.info(
                        f"Đã phân tích xu hướng công nghệ cho {subreddit_trends['subreddit_name'].nunique()} subreddit")
                else:
                    logger.info("Không có dữ liệu xu hướng theo subreddit")

            elif args.specific_analysis == 'sentiment_popularity':
                sentiment_pop_corr = analyzer.analyze_sentiment_popularity_correlation(min_mentions=args.min_mentions)
                logger.info(f"Đã phân tích tương quan sentiment-popularity cho {len(sentiment_pop_corr)} công nghệ")

            elif args.specific_analysis == 'time_trends':
                time_trends = analyzer.analyze_tech_trends_by_time(
                    time_unit=args.time_unit,
                    min_mentions=args.min_mentions
                )
                if not time_trends.empty:
                    logger.info(
                        f"Đã phân tích xu hướng theo {args.time_unit} cho {time_trends['tech_name'].nunique()} công nghệ")
                else:
                    logger.info(f"Không có dữ liệu xu hướng theo {args.time_unit}")

    except Exception as e:
        logger.error(f"Lỗi trong quá trình phân tích xu hướng: {str(e)}")
    finally:
        # Đảm bảo đóng kết nối
        if analyzer:
            analyzer.close()

    # Tính thời gian thực thi
    end_time = time.time()
    duration = end_time - start_time
    hours, remainder = divmod(duration, 3600)
    minutes, seconds = divmod(remainder, 60)

    logger.info("Hoàn thành quá trình phân tích xu hướng")
    logger.info(f"Thời gian thực thi: {int(hours)} giờ {int(minutes)} phút {int(seconds)} giây")


if __name__ == "__main__":
    main()