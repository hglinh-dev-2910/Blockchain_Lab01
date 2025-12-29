import argparse
import sys
import os

# Thêm thư mục src vào đường dẫn tìm kiếm module
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.node import RaftNode

def main():
    parser = argparse.ArgumentParser(description='Run a Raft Node')
    parser.add_argument('--id', type=int, required=True, help='ID of the node (1-5)')
    args = parser.parse_args()

    node_id = args.id
    
    # Kiểm tra ID hợp lệ
    if node_id < 1 or node_id > 5:
        print("Error: Node ID must be between 1 and 5")
        sys.exit(1)

    print(f"Starting Node {node_id}...")
    
    # Khởi tạo và chạy Node
    node = RaftNode(node_id)
    node.start()

if __name__ == '__main__':
    main()