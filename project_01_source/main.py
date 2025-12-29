import argparse
import sys
import os
import threading
import time

# Thêm thư mục src vào đường dẫn tìm kiếm module
sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))

from src.node import RaftNode

def handle_user_input(node):
    """Luồng xử lý nhập liệu từ bàn phím để gửi lệnh test"""
    time.sleep(1.5) # Chờ log khởi động chạy xong cho đỡ rối
    print("\n" + "="*50)
    print(f"Interactive Mode for Node {node.node_id}")
    print("Commands:")
    print("  SET <key> <value>  : Submit data to Raft (Only works on Leader)")
    print("  STATE              : Show current node state")
    print("  LOG                : Show committed logs")
    print("  DB                 : Show current Key-Value DB")
    print("  EXIT               : Stop node")
    print("="*50 + "\n")

    print(f"Node-{node.node_id}> ", end='', flush=True)

    while True:
        try:
            # Input blocking
            cmd = input() 
            print(f"Node-{node.node_id}> ", end='', flush=True)
            
            parts = cmd.strip().split()
            if not parts: continue
            
            op = parts[0].upper()
            
            if op == "EXIT":
                print("Stopping node...")
                os._exit(0)
            
            elif op == "STATE":
                print(f"\n--- Node {node.node_id} State ---")
                print(f"Role: {node.state}")
                print(f"Term: {node.current_term}")
                print(f"Commit Index: {node.commit_index}")
                print(f"Voted For: {node.voted_for}")
                print(f"Blocked IPs: {node.blocked_ips}")
                print("-----------------------------")

            elif op == "DB":
                print(f"\n--- Database Content ---\n{node.db}\n------------------------")

            elif op == "LOG":
                 print(f"\n--- Log (Length: {len(node.log)}) ---")
                 for i, entry in enumerate(node.log):
                     print(f"  [{i+1}] Term {entry.term}: {entry.command}")
                 print("------------------------------")

            elif op == "SET":
                # Ví dụ: SET username admin
                full_cmd = " ".join(parts)
                success, msg = node.submit_command(full_cmd)
                if success:
                    print(f"\n✅ [Success]: {msg}")
                else:
                    print(f"\n❌ [Failed]: {msg}")
            
            else:
                print("\nUnknown command. Try SET, STATE, LOG, DB, EXIT.")
                
        except EOFError:
            break
        except Exception as e:
            print(f"Error processing input: {e}")

def main():
    parser = argparse.ArgumentParser(description='Run a Raft Node')
    parser.add_argument('--id', type=int, required=True, help='ID of the node (1-5)')
    args = parser.parse_args()

    node_id = args.id
    if node_id < 1 or node_id > 5:
        print("Error: Node ID must be between 1 and 5")
        sys.exit(1)

    # Khởi tạo Node
    node = RaftNode(node_id)
    
    # Chạy luồng nhận input từ bàn phím
    input_thread = threading.Thread(target=handle_user_input, args=(node,), daemon=True)
    input_thread.start()

    # Chạy Node (Block main thread)
    node.start()

if __name__ == '__main__':
    main()