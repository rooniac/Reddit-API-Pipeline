import os
import logging
import sys
from logging.handlers import RotatingFileHandler
from .config import LOG_LEVEL, LOG_FORMAT

def setup_logger(name, log_file=None):
    """
        Thiết lập logger với format và level được định nghĩa trong config

        Args:
            name (str): Tên của logger
            log_file (str, optional): Đường dẫn tới file log

        Returns:
            logging.Logger: Logger đã được cấu hình
    """

    # Create logger
    logger = logging.getLogger(name)
    logger.setLevel(getattr(logging, LOG_LEVEL))

    # Create formatter
    formatter = logging.Formatter(LOG_FORMAT)

    # Create console handler and formatter
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # If log file exist, create file handler
    if log_file:
        # Make sure logs folder exist
        os.makedirs(os.path.dirname(log_file), exist_ok=True)

        # Create rotating file handler (limit size and number of file)
        file_handler = RotatingFileHandler(
            log_file, maxBytes=10*1024*1024, backupCount=5,
            encoding='utf-8'
        )

        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

