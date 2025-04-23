
import sys
import os
import argparse
import traceback

# Thêm thư mục gốc của dự án vào sys.path để có thể import các module
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from src.data_visualization.dashboard import RedditDashboard
from src.utils.logger import setup_logger

# Thiết lập logger
logger = setup_logger("dashboard_runner", "logs/dashboard_runner.log")


def main():
    """
    Hàm chính để chạy dashboard visualize
    """

    # Xử lý tham số dòng lệnh
    parser = argparse.ArgumentParser(description='Chạy Dashboard Reddit Data Engineering')
    parser.add_argument('--host', type=str, default='0.0.0.0', help='Host address')
    parser.add_argument('--port', type=int, default=8050, help='Port number')
    parser.add_argument('--debug', action='store_true', help='Debug mode')
    args = parser.parse_args()

    logger.info("Bắt đầu khởi động dashboard visualize")

    dashboard = None
    try:
        # Khởi tạo dashboard
        dashboard = RedditDashboard(debug=args.debug)

        # Chạy server Dash (sử dụng app.run thay vì app.run_server)
        logger.info(f"Chạy dashboard trên {args.host}:{args.port}")
        dashboard.run_server(host=args.host, port=args.port, debug=args.debug)

    except Exception as e:
        logger.error(f"Lỗi khi chạy dashboard: {str(e)}")
        # In ra stack trace để giúp debug
        logger.error(traceback.format_exc())
    finally:
        # Đảm bảo đóng mọi kết nối
        if dashboard:
            dashboard.shutdown()

    logger.info("Dashboard đã dừng hoạt động")


if __name__ == "__main__":
    main()