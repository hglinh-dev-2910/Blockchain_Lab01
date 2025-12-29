import grpc
import sys
import os
import argparse
import json

sys.path.append(os.path.join(os.path.dirname(__file__), 'src'))
import src.raft_pb2 as raft_pb2
import src.raft_pb2_grpc as raft_pb2_grpc

def load_config(path='config.json'):
    with open(path, 'r') as f:
        return json.load(f)

def set_partition(target_node_id, blocked_ips):
    config = load_config()
    target = next((n for n in config['nodes'] if n['id'] == target_node_id), None)
    
    if not target:
        print(f"Node {target_node_id} not found in config")
        return

    addr = f"{target['ip']}:{target['port']}"
    # print(f"Connecting to Node {target_node_id} at {addr}...")
    
    try:
        channel = grpc.insecure_channel(addr)
        stub = raft_pb2_grpc.RaftServiceStub(channel)
        
        request = raft_pb2.PartitionArgs(blocked_ips=blocked_ips)
        response = stub.SetPartition(request)
        
        if response.success:
            if not blocked_ips:
                 print(f"✅ Node {target_node_id} is now FULLY CONNECTED (No blocks).")
            else:
                 print(f"✅ Node {target_node_id} is now blocking: {blocked_ips}")
        else:
            print(f"❌ Failed to update partition for Node {target_node_id}")
            
    except grpc.RpcError as e:
        print(f"❌ RPC Error connecting to {target_node_id}: {e}")

def apply_split(group1_str, group2_str):
    """
    Tự động chia mạng thành 2 nhóm tách biệt hoàn toàn (chặn 2 chiều).
    Usage: --split "1,2" "3,4,5"
    """
    config = load_config()
    
    # Parse input string "1,2" -> list [1, 2]
    try:
        g1_ids = [int(x.strip()) for x in group1_str.split(',')]
        g2_ids = [int(x.strip()) for x in group2_str.split(',')]
    except ValueError:
        print("❌ Error: Group IDs must be integers separated by commas (e.g., '1,2')")
        return

    print(f"\n⚡ Applying Network Split: Group {g1_ids} <---> Group {g2_ids}\n")

    # Lấy danh sách IP:Port của từng nhóm
    g1_ips = []
    g2_ips = []
    
    for node in config['nodes']:
        addr = f"{node['ip']}:{node['port']}"
        if node['id'] in g1_ids:
            g1_ips.append(addr)
        elif node['id'] in g2_ids:
            g2_ips.append(addr)

    # 1. Cấu hình cho Group 1: Chặn toàn bộ IP của Group 2
    for nid in g1_ids:
        set_partition(nid, g2_ips)

    # 2. Cấu hình cho Group 2: Chặn toàn bộ IP của Group 1
    for nid in g2_ids:
        set_partition(nid, g1_ips)
        
    print("\n✅ Network partition applied successfully.")

def heal_network():
    """Xóa toàn bộ quy tắc chặn trên tất cả các node"""
    print("\n🚑 Healing Network (Unblocking all nodes)...\n")
    config = load_config()
    for node in config['nodes']:
        set_partition(node['id'], [])
    print("\n✅ Network healed. All nodes can communicate.")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Test Network Partition')
    
    # Mode 1: Manual target
    parser.add_argument('--target', type=int, help='Node ID to apply partition rules')
    parser.add_argument('--block', nargs='*', help='List of IPs to block', default=[])
    
    # Mode 2: Auto split
    parser.add_argument('--split', nargs=2, help='Split network into two groups. Usage: --split "1,2" "3,4,5"')
    
    # Mode 3: Heal
    parser.add_argument('--heal', action='store_true', help='Unblock all nodes (Restore network)')
    
    args = parser.parse_args()
    
    if args.heal:
        heal_network()
    elif args.split:
        apply_split(args.split[0], args.split[1])
    elif args.target:
        set_partition(args.target, args.block)
    else:
        parser.print_help()