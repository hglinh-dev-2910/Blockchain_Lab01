import logging
import os
import sys

def get_logger(node_id):
    """
    Tạo logger riêng cho từng Node, vừa in ra màn hình vừa ghi vào file.
    """
    logger = logging.getLogger(f"Node-{node_id}")
    logger.setLevel(logging.DEBUG)

    # Định dạng log: [Thời gian] [Level] [Tên Node]: Nội dung
    formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(name)s: %(message)s', datefmt='%H:%M:%S')

    # Handler 1: In ra màn hình (Console)
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)

    # Handler 2: Ghi vào file logs/node_X.log
    if not os.path.exists('logs'):
        os.makedirs('logs')
    
    file_handler = logging.FileHandler(f"logs/node_{node_id}.log")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger