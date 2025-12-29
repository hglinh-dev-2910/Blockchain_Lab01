import grpc
from concurrent import futures
import time
import threading
import random
import json
import sys
import os

# Hack để python tìm thấy module trong thư mục hiện tại
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
        self.peers = self.config['nodes']
        
        self.logger = get_logger(node_id)
        
        # --- Persistent State ---
        self.current_term = 0
        self.voted_for = None
        self.log = [] 
        
        # Key-Value Store
        self.db = {} 

        # --- Volatile State ---
        self.commit_index = 0
        self.last_applied = 0
        self.state = FOLLOWER
        self.leader_id = None
        
        # --- Volatile State (Leader Only) ---
        self.next_index = {}
        self.match_index = {}
        
        self.lock = threading.Lock()
        
        # --- Timers ---
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

    # --- Helper Methods ---
    def get_last_log_index(self):
        return len(self.log)

    def get_last_log_term(self):
        if self.log:
            return self.log[-1].term
        return 0

    def get_term_at_index(self, index):
        if index == 0: return 0
        if index > len(self.log): return 0
        return self.log[index - 1].term

    def is_peer_blocked(self, peer_id=None, peer_addr=None):
        """Kiểm tra chặn 2 chiều: Dùng cho cả nhận và gửi"""
        try:
            if peer_id:
                for p in self.peers:
                    if p['id'] == peer_id:
                        peer_addr = f"{p['ip']}:{p['port']}"
                        break
            
            if peer_addr and peer_addr in self.blocked_ips:
                return True
        except Exception:
            return False
        return False

    def start(self):
        server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
        raft_pb2_grpc.add_RaftServiceServicer_to_server(self, server)
        
        address = self.get_my_address()
        server.add_insecure_port(address)
        server.start()
        
        self.logger.info(f"Server started listening on {address}")
        
        threading.Thread(target=self.run_election_timer, daemon=True).start()
        threading.Thread(target=self.run_apply_loop, daemon=True).start()
        
        try:
            while True:
                time.sleep(1)
        except KeyboardInterrupt:
            server.stop(0)
            self.logger.info("Server stopped.")

    def run_election_timer(self):
        while True:
            try:
                time.sleep(0.1)
                with self.lock:
                    if self.state == LEADER:
                        continue
                    
                    elapsed = time.time() - self.last_heartbeat
                    if elapsed > self.election_timeout:
                        self.logger.warning(f"Election timeout ({elapsed:.2f}s). Starting election...")
                        self.start_election()
                        # Randomize timeout again to avoid split votes repeating exactly
                        self.election_timeout = random.uniform(3.0, 6.0)
            except Exception as e:
                self.logger.error(f"Error in election timer: {e}")

    def run_apply_loop(self):
        while True:
            try:
                time.sleep(0.1)
                with self.lock:
                    while self.last_applied < self.commit_index:
                        self.last_applied += 1
                        entry = self.log[self.last_applied - 1]
                        self.apply_to_db(entry.command)
            except Exception as e:
                self.logger.error(f"Error in apply loop: {e}")

    def apply_to_db(self, command):
        try:
            parts = command.split()
            op = parts[0].upper()
            if op == "SET" and len(parts) >= 3:
                key = parts[1]
                val = " ".join(parts[2:])
                self.db[key] = val
                self.logger.info(f"Checking committed entry... APPLIED TO DB: {key} = {val}")
        except Exception:
            pass

    def submit_command(self, command):
        with self.lock:
            if self.state != LEADER:
                return False, f"Not Leader. Leader is {self.leader_id}"
            
            new_entry = raft_pb2.LogEntry(term=self.current_term, command=command)
            self.log.append(new_entry)
            self.logger.info(f"Client command received. Appended to index {len(self.log)}")
            return True, "Command submitted"

    # --- Election Logic ---

    def start_election(self):
        self.state = CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id
        self.last_heartbeat = time.time()
        self.votes_received = 1
        
        self.logger.info(f"Became CANDIDATE. Term: {self.current_term}. Requesting votes...")
        
        for peer in self.peers:
            if peer['id'] != self.node_id:
                threading.Thread(target=self.send_request_vote, args=(peer,), daemon=True).start()

    def send_request_vote(self, peer):
        addr = f"{peer['ip']}:{peer['port']}"
        
        # [FIX] Kiểm tra nếu node này bị chặn thì không gửi (Outgoing Check)
        if self.is_peer_blocked(peer_addr=addr):
            return

        try:
            channel = grpc.insecure_channel(addr)
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            
            with self.lock:
                last_log_idx = self.get_last_log_index()
                last_log_term = self.get_last_log_term()
                request = raft_pb2.RequestVoteArgs(
                    term=self.current_term,
                    candidateId=self.node_id,
                    lastLogIndex=last_log_idx,
                    lastLogTerm=last_log_term
                )
            
            response = stub.RequestVote(request, timeout=1.0)
            
            with self.lock:
                if response.term > self.current_term:
                    self.current_term = response.term
                    self.state = FOLLOWER
                    self.voted_for = None
                    return

                if self.state == CANDIDATE and response.voteGranted:
                    self.votes_received += 1
                    if self.votes_received > len(self.peers) / 2:
                        self.become_leader()
                        
        except Exception:
            pass

    def become_leader(self):
        if self.state == LEADER: return
        self.state = LEADER
        self.leader_id = self.node_id
        
        for peer in self.peers:
            pid = peer['id']
            self.next_index[pid] = self.get_last_log_index() + 1
            self.match_index[pid] = 0
            
        self.logger.info(f"*** BECAME LEADER (Term {self.current_term}) ***")
        threading.Thread(target=self.run_heartbeat_loop, daemon=True).start()

    def run_heartbeat_loop(self):
        while self.state == LEADER:
            self.send_heartbeats()
            time.sleep(0.5) 

    def send_heartbeats(self):
        for peer in self.peers:
            if peer['id'] != self.node_id:
                threading.Thread(target=self.send_append_entries, args=(peer,), daemon=True).start()

    def send_append_entries(self, peer):
        addr = f"{peer['ip']}:{peer['port']}"
        pid = peer['id']
        
        # [FIX] Kiểm tra nếu node này bị chặn thì không gửi (Outgoing Check)
        if self.is_peer_blocked(peer_addr=addr):
            return

        with self.lock:
            if self.state != LEADER: return
            
            prev_log_index = self.next_index.get(pid, 1) - 1
            prev_log_term = self.get_term_at_index(prev_log_index)
            
            entries_to_send = []
            if self.get_last_log_index() >= self.next_index.get(pid, 1):
                entries_to_send = self.log[self.next_index[pid]-1:]

            proto_entries = [raft_pb2.LogEntry(term=e.term, command=e.command) for e in entries_to_send]

            request = raft_pb2.AppendEntriesArgs(
                term=self.current_term,
                leaderId=self.node_id,
                prevLogIndex=prev_log_index,
                prevLogTerm=prev_log_term,
                entries=proto_entries,
                leaderCommit=self.commit_index
            )

        try:
            channel = grpc.insecure_channel(addr)
            stub = raft_pb2_grpc.RaftServiceStub(channel)
            response = stub.AppendEntries(request, timeout=0.5)
            
            with self.lock:
                if response.term > self.current_term:
                    self.logger.info(f"Leader discovered higher term {response.term} from {pid}. Stepping down.")
                    self.current_term = response.term
                    self.state = FOLLOWER
                    self.voted_for = None
                    return

                if response.success:
                    if len(entries_to_send) > 0:
                        self.next_index[pid] = prev_log_index + len(entries_to_send) + 1
                        self.match_index[pid] = prev_log_index + len(entries_to_send)
                        self.update_commit_index()
                else:
                    self.next_index[pid] = max(1, self.next_index[pid] - 1)

        except Exception:
            pass

    def update_commit_index(self):
        for N in range(self.get_last_log_index(), self.commit_index, -1):
            count = 1 
            for peer in self.peers:
                if peer['id'] != self.node_id and self.match_index.get(peer['id'], 0) >= N:
                    count += 1
            
            if count > len(self.peers) / 2:
                if self.get_term_at_index(N) == self.current_term:
                    self.commit_index = N
                    self.logger.info(f"Leader committed index {self.commit_index}")
                    break

    # --- gRPC Handlers ---

    def RequestVote(self, request, context):
        with self.lock:
            # [FIX] Server-side check
            if self.is_peer_blocked(peer_id=request.candidateId):
                return raft_pb2.RequestVoteReply(term=self.current_term, voteGranted=False)

            reply = raft_pb2.RequestVoteReply(term=self.current_term, voteGranted=False)
            
            if request.term > self.current_term:
                self.current_term = request.term
                self.state = FOLLOWER
                self.voted_for = None

            if request.term < self.current_term:
                return reply

            my_last_idx = self.get_last_log_index()
            my_last_term = self.get_last_log_term()
            
            log_is_ok = (request.lastLogTerm > my_last_term) or \
                        (request.lastLogTerm == my_last_term and request.lastLogIndex >= my_last_idx)

            if (self.voted_for is None or self.voted_for == request.candidateId) and log_is_ok:
                self.voted_for = request.candidateId
                self.last_heartbeat = time.time()
                reply.voteGranted = True
                self.logger.info(f"Voted YES for Node {request.candidateId} in Term {self.current_term}")
            
            return reply

    def AppendEntries(self, request, context):
        with self.lock:
            # [FIX] Server-side check
            if self.is_peer_blocked(peer_id=request.leaderId):
                return raft_pb2.AppendEntriesReply(term=self.current_term, success=False)

            reply = raft_pb2.AppendEntriesReply(term=self.current_term, success=False)
            
            if request.term > self.current_term:
                self.current_term = request.term
                self.state = FOLLOWER
                self.leader_id = request.leaderId
                self.voted_for = None
            
            if request.term < self.current_term:
                return reply

            self.last_heartbeat = time.time()
            self.leader_id = request.leaderId
            if self.state != FOLLOWER:
                self.state = FOLLOWER
            
            if request.prevLogIndex > self.get_last_log_index():
                return reply 
            
            if request.prevLogIndex > 0:
                existing_term = self.get_term_at_index(request.prevLogIndex)
                if existing_term != request.prevLogTerm:
                    self.log = self.log[:request.prevLogIndex-1]
                    return reply
            
            current_idx = request.prevLogIndex
            for entry in request.entries:
                current_idx += 1
                if current_idx <= self.get_last_log_index():
                    if self.get_term_at_index(current_idx) != entry.term:
                        self.log = self.log[:current_idx-1]
                        self.log.append(entry)
                else:
                    self.log.append(entry)
            
            if request.leaderCommit > self.commit_index:
                self.commit_index = min(request.leaderCommit, self.get_last_log_index())

            reply.success = True
            return reply

    def SetPartition(self, request, context):
        with self.lock:
            self.blocked_ips = request.blocked_ips
            self.logger.warning(f"Partition updated. Blocked: {self.blocked_ips}")
            return raft_pb2.PartitionReply(success=True)