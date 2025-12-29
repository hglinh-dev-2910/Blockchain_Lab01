import grpc
from concurrent import futures
import time
import threading
import random
import json
import sys
import os

# Hack để python tìm thấy module trong thư mục src khi chạy từ bên ngoài
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

import raft_pb2
import raft_pb2_grpc
from utils import get_logger

# Các hằng số trạng thái
FOLLOWER = 'FOLLOWER'
CANDIDATE = 'CANDIDATE'
LEADER = 'LEADER'

class RaftNode(raft_pb2_grpc.RaftServiceServicer):
    def __init__(self, node_id, config_path='config.json'):
        self.node_id = node_id
        self.config = self.load_config(config_path)
        self.peers = self.config['nodes'] # Danh sách các node khác
        
        # Setup Logger
        self.logger = get_logger(node_id)
        
        # --- Persistent State (Lưu trữ bền vững) ---
        self.current_term = 0
        self.voted_for = None
        self.log = [] # Chứa LogEntry
        
        # --- Volatile State (Trạng thái tạm thời) ---
        self.commit_index = 0
        self.last_applied = 0
        self.state = FOLLOWER
        self.leader_id = None
        
        # --- Volatile State (Leader Only) ---
        self.next_index = {}
        self.match_index = {}
        
        # --- Timers ---
        # Timeout ngẫu nhiên từ 150ms - 300ms (nhân với hệ số để dễ quan sát hơn, ví dụ 3s-6s khi debug)
        self.election_timeout = random.uniform(3.0, 6.0) 
        self.last_heartbeat = time.time()
        
        # Network Partition Simulation
        self.blocked_ips = []
        
        self.logger.info(f"Node initialized. State: {self.state}, Term: {self.current_term}")

    def load_config(self, path):
        with open(path, 'r') as f:
            return json.load(f)

    def get_my_address(self):
        for node in self.peers:
            if node['id'] == self.node_id:
                return f"{node['ip']}:{node['port']}"
        return None

    def start(self):
        """Khởi động gRPC Server và luồng kiểm tra timeout"""
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftServiceServicer_to_server(self, server)
        
        address = self.get_my_address()
        server.add_insecure_port(address)
        server.start()
        
        self.logger.info(f"Server started listening on {address}")
        
        # Chạy luồng Election Timer riêng biệt
        election_thread = threading.Thread(target=self.run_election_timer, daemon=True)
        election_thread.start()
        
        try:
            # Giữ main thread sống để server hoạt động
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            server.stop(0)
            self.logger.info("Server stopped.")

    def run_election_timer(self):
        """Vòng lặp kiểm tra xem có cần bầu cử không"""
        while True:
            time.sleep(0.1) # Check mỗi 100ms
            
            if self.state == FOLLOWER or self.state == CANDIDATE:
                elapsed_time = time.time() - self.last_heartbeat
                if elapsed_time > self.election_timeout:
                    self.logger.warning(f"Election timeout ({elapsed_time:.2f}s > {self.election_timeout:.2f}s). Starting election...")
                    self.start_election()

    def start_election(self):
        """Chuyển sang Candidate và bắt đầu bầu cử"""
        self.state = CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id # Tự bầu cho mình
        self.last_heartbeat = time.time() # Reset timer
        
        # TODO: Gửi RequestVote RPC đến tất cả các node khác (Sẽ code ở bước sau)
        self.logger.info(f"Became CANDIDATE. Term: {self.current_term}. Requesting votes...")

    # --- gRPC Implementation ---

    def RequestVote(self, request, context):
        """Nhận yêu cầu bầu cử từ Candidate khác"""
        self.logger.debug(f"Received RequestVote from Node {request.candidateId} for Term {request.term}")
        
        # TODO: Implement logic check term và log (Sẽ code ở bước sau)
        
        return raft_pb2.RequestVoteReply(term=self.current_term, voteGranted=False)

    def AppendEntries(self, request, context):
        """Nhận log hoặc heartbeat từ Leader"""
        # Nếu nhận được heartbeat từ Leader hợp lệ -> Reset timeout
        if request.term >= self.current_term:
            self.last_heartbeat = time.time()
            if self.state != FOLLOWER:
                self.state = FOLLOWER
                self.logger.info(f"Stepping down to FOLLOWER. Leader is {request.leaderId}")
            
            self.leader_id = request.leaderId
            
        return raft_pb2.AppendEntriesReply(term=self.current_term, success=True)
        
    def SetPartition(self, request, context):
        """API để giả lập lỗi mạng (Network Partition)"""
        self.blocked_ips = request.blocked_ips
        self.logger.warning(f"Partition updated. Blocking IPs: {self.blocked_ips}")
        return raft_pb2.PartitionReply(success=True)