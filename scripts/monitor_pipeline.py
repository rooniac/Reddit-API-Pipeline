# scripts/monitor_pipeline.py
import os
import re
import glob
import smtplib
from email.message import EmailMessage
from datetime import datetime, timedelta


def check_logs(log_dir="logs", hours=24):
    """
    Kiểm tra các file log để tìm lỗi trong khoảng thời gian chỉ định

    Args:
        log_dir (str): Thư mục chứa file log
        hours (int): Số giờ để kiểm tra ngược lại từ thời điểm hiện tại

    Returns:
        dict: Các lỗi được tìm thấy theo file
    """
    errors = {}
    cutoff_time = datetime.now() - timedelta(hours=hours)

    # Lấy tất cả các file log
    log_files = glob.glob(os.path.join(log_dir, "*.log"))

    for log_file in log_files:
        file_errors = []

        try:
            with open(log_file, 'r', encoding='utf-8') as f:
                for line in f:
                    # Tìm dòng có chứa timestamp và ERROR
                    match = re.search(r'(\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3}) - .* - ERROR - (.*)', line)
                    if match:
                        timestamp_str, error_msg = match.groups()
                        try:
                            timestamp = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S,%f')
                            if timestamp >= cutoff_time:
                                file_errors.append((timestamp, error_msg))
                        except ValueError:
                            # Nếu không phân tích được timestamp, vẫn giữ lại lỗi
                            file_errors.append((datetime.now(), error_msg))
        except Exception as e:
            file_errors.append((datetime.now(), f"Không thể đọc file log: {str(e)}"))

        if file_errors:
            errors[os.path.basename(log_file)] = file_errors

    return errors


def send_error_notification(errors, email_to):
    """
    Gửi email thông báo lỗi

    Args:
        errors (dict): Các lỗi được tìm thấy theo file
        email_to (str): Địa chỉ email nhận thông báo
    """
    if not errors:
        return

    # Tạo nội dung email
    msg = EmailMessage()
    msg['Subject'] = f'[Reddit Pipeline] Phát hiện lỗi - {datetime.now().strftime("%Y-%m-%d %H:%M")}'
    msg['From'] = 'your_email@example.com'  # Thay bằng email của bạn
    msg['To'] = email_to

    content = "Các lỗi sau đã được phát hiện trong pipeline Reddit:\n\n"

    for log_file, file_errors in errors.items():
        content += f"=== {log_file} ===\n"
        for timestamp, error_msg in file_errors:
            content += f"[{timestamp.strftime('%Y-%m-%d %H:%M:%S')}] {error_msg}\n"
        content += "\n"

    msg.set_content(content)

    # Gửi email (bạn cần cấu hình SMTP server)
    # Đoạn code này chỉ là ví dụ, bạn có thể bỏ qua nếu không cần gửi email
    try:
        with smtplib.SMTP('smtp.example.com', 587) as server:  # Thay bằng SMTP server của bạn
            server.starttls()
            server.login('your_email@example.com', 'your_password')  # Thay bằng thông tin đăng nhập của bạn
            server.send_message(msg)
            print(f"Đã gửi thông báo lỗi đến {email_to}")
    except Exception as e:
        print(f"Không thể gửi email: {str(e)}")
        # Vẫn in ra lỗi để kiểm tra
        print(content)


def main():
    """
    Hàm chính để kiểm tra log và gửi thông báo nếu cần
    """
    errors = check_logs(hours=24)  # Kiểm tra lỗi trong 24 giờ qua

    if errors:
        # In lỗi ra console
        for log_file, file_errors in errors.items():
            print(f"=== {log_file} ===")
            for timestamp, error_msg in file_errors:
                print(f"[{timestamp.strftime('%Y-%m-%d %H:%M:%S')}] {error_msg}")
            print()

        # Gửi thông báo
        # send_error_notification(errors, "your_email@example.com")  # Bỏ comment nếu muốn sử dụng


if __name__ == "__main__":
    main()