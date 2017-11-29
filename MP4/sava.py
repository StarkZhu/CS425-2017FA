from global_vars import *
from utils import *
from threading import Lock
from threading import Thread
from collections import defaultdict
from collections import deque
import time
import sys

class SavaMaster():
    def __init__(self):
        self._workers = {}
        self.finish_cnt = 0
        self.has_update = False
        self._iter_cnt = 0
        self.max_iter = float('inf')

    def initialize(self, args, members):
        self._iter_cnt = 0
        if args[0] == 'pgrk':
            self.max_iter = int(args[1]) + 2
        self.set_workers(members)
        self.init_work(args)

    def set_workers(self, members):
        cnt = 0
        for m in members:
            mid = int(m[0].split('.')[0].split('-')[-1])
            if mid <= 3:
                continue

            self._workers[str(cnt)] = m[0]
            cnt += 1

    def finish_iteration(self, worker_id, updated):
        self.finish_cnt += 1

        print('worker %s finish iteration called' % worker_id)
        self.has_update = updated or self.has_update

        if self.finish_cnt == len(self._workers):
            p = Thread(target=self.init_next_iter)
            p.start()
        elif self.finish_cnt == len(self._workers)*2:

            # work is complete
            print('current iteration %d' % self._iter_cnt)
            self.finish_cnt = 0
            self._iter_cnt += 1
            
            # call start iteration
            if (self.has_update and self._iter_cnt < self.max_iter):
                p = Thread(target=self.proceed_next_iter)
                p.start()
            else:
                p = Thread(target=self.finish)
                p.start()
            self.has_update = False

    def finish(self):
        for worker_ip in self._workers.values():
            handle = get_tcp_client_handle(worker_ip)
            handle.finish_work()

    def init_next_iter(self):
        for worker_ip in self._workers.values():
            handle = get_tcp_client_handle(worker_ip)
            handle.init_next_iter()

    def proceed_next_iter(self):
        for worker_ip in self._workers.values():
            handle = get_tcp_client_handle(worker_ip)
            handle.proceed_next_iter()

    def init_work(self, args):
        print('master start initializing')
        print(self._workers)
        for worker_id, worker_ip in self._workers.items():
            handle = get_tcp_client_handle(worker_ip)
            handle.init_sava_worker(worker_id, args, self._workers)


class SavaWorker():
    def __init__(self):
        self.worker_id = ''
        # map node_number to list
        self._msgin = defaultdict(list)
        self._msgout = defaultdict(list)
        self._graph = defaultdict(list)
        self._nodeValue = {}
        self._lock = Lock()
        self.peers = None
        self.master = INTRODUCER

        self.task = None
        self.last_round_msgin = {}
        self.partition = {}

    def set_slave(self, slave):
        self._slave = slave

    def initialize(self, worker_id, args, workers):
        self.worker_id = worker_id
        self.set_peers(workers)

        print('Worker %s initializing' % self.worker_id)

        if args[0] == 'sssp':
            self.task = 'sssp'
            p = Thread(target=self.load_graph, args=[float('inf')])
            p.start()

            if self.node_belong_to(source_node) == self.worker_id:
                self.store_to_msgin({str(source_node) : [0]})

        elif args[0] == 'pgrk':
            self.task = 'pgrk'
            p = Thread(target=self.load_graph, args=[1])
            p.start()

        print('worker init done')
        
    def set_master(self, master):
        self.master = master

    def set_worker_id(self, id):
        self.worker_id = id

    def set_peers(self, peers):
        # id:ip
        self.peers = peers

    def load_graph(self, init_value):
        num_nodes = 0
        num_edges = 0

        print('loading graph STARTED')
        cur_time = time.time()
        # get graph from sdfs 

        self._slave.get(SAVA_GRAPH_FILE, SAVA_GRAPH_FILE)

        # read graph
        with open(SAVA_GRAPH_FILE) as fin:
            for cnt, line in enumerate(fin):

                splits = line.strip().split('\t')
                v1 = splits[0]
                v2 = splits[1]
                self._graph[v1].append(v2)
                self._graph[v2].append(v1)

        print('loading graph DONE takes {}s'.format(time.time() - cur_time))

        cur_time = time.time()
        self.bfs_partition()
        print('partition DONE takes {}s'.format(time.time() - cur_time))

        for node in self._graph.keys():
            if (self.node_belong_to(node) == self.worker_id):
                self._nodeValue[node] = init_value

        self.reach_barrier(True)

    def bfs_partition(self):
        q = deque()
        sorted_nodes = []

        q.append('1')
        sorted_nodes.append('1')

        unvisited = set(self._graph.keys())
        while len(q) > 0:
            # print(len(q), len(unvisited))
            node = q.popleft()
            if node not in unvisited:
                continue
            #q += self._graph[node]
            for neighbor in self._graph[node]:
                if neighbor in unvisited:
                    q.append(neighbor)
            sorted_nodes.append(node)
            unvisited.remove(node)

        # start = len(sorted_nodes) / 
        # self.partition = {}
        print('start to save partition')
        for i, vertex in enumerate(sorted_nodes):
            self.partition[vertex] = int(i * len(self.peers) / len(sorted_nodes))

        print(len(self.partition))

    def node_belong_to(self, node):
        return str(self.partition[node])
    # def node_belong_to(self, node):
    #     return str(int(node) % len(self.peers))

    def store_to_msgin_thread(self, msg):
        self._lock.acquire()
        for node in msg.keys():
            self._msgin[node] += msg[node]
        self._lock.release()

    def store_to_msgin(self, msg):
        # cur_time = time.time()
        p = Thread(target=self.store_to_msgin_thread, args=[msg])
        p.start()
        # print('create thread to store_to_msgin takes {}s'.format(time.time() - cur_time))

    def save_result(self):
        filename = '%s_sava_output' % self.worker_id
        with open(filename, 'w+') as fout:
            for key, value in self._nodeValue.items():
                fout.write('{} {}\n'.format(key, value))

        self._slave.put(filename, filename)

    def process(self):
        if self.task == 'sssp':
            p = Thread(target=self.process_SSSP)
            p.start()
        elif self.task == 'pgrk':
            p = Thread(target=self.process_PGRK)
            p.start()
        else:
            print('Task Not Valid.')
    
    def gather(self):
        p = Thread(target=self.gather_msg)
        p.start()

    def gather_msg(self):
        print('start gather msg')
        self._msgout = defaultdict(dict)
        self._lock.acquire()
        self.last_round_msgin = self._msgin
        self._msgin = defaultdict(list)
        self._lock.release()
        self.reach_barrier(True)    

    def process_PGRK(self):
        print('start processing')

        cur_time = time.time()
        for vertex, msg in self.last_round_msgin.items():
            ranks = sum(msg)
            self._nodeValue[vertex] = 0.85*ranks + 0.15
        print('G and A takes {}s'.format(time.time() - cur_time))
        cur_time = time.time()

        for vertex in self._nodeValue.keys():
            contribs = self._nodeValue[vertex]*1.0/len(self._graph[vertex])
            for neighbor in self._graph[vertex]:
                send_to_id = self.node_belong_to(neighbor)
                if neighbor not in self._msgout[send_to_id]:
                    self._msgout[send_to_id][neighbor] = [contribs]
                else:
                    self._msgout[send_to_id][neighbor][0] += contribs
        print('Prepare out package taks {}s'.format(time.time() - cur_time))
        cur_time = time.time()

        print('calculation finished')
        threads = []
        for send_to_id in self._msgout.keys():
            # print('msg send to {} size: {}'.format(send_to_id, sys.getsizeof(self._msgout[send_to_id])))
            p = Thread(target=self.send_msg_thread, args=[send_to_id])
            p.start()
            threads.append(p)

        for p in threads:
            p.join()

        print('send finished, takes {}s'.format(time.time() - cur_time))     
        cur_time = time.time()

        self.reach_barrier(True)
        print('wait for barrier takes{}s'.format(time.time() - cur_time))

    def send_msg_thread(self, send_to_id):
        print('msg send to {} size: {}'.format(send_to_id, sys.getsizeof(str(self._msgout[send_to_id]))))
        if send_to_id == self.worker_id:
            self.store_to_msgin(self._msgout[send_to_id])
            return
        handle = get_tcp_client_handle(self.peers[send_to_id])
        handle.sava_transfer_data(self._msgout[send_to_id])
        #handle.sava_transfer_data({})

    def process_SSSP(self):
        '''
        Thread that runs calculation
        '''
        print('start processing')
        updated = False;
        self._msgout = defaultdict(dict)
        self._lock.acquire()
        last_round_msgin = self._msgin
        self._msgin = defaultdict(list)
        self._lock.release()

        for vertex, msg in last_round_msgin.items():
            min_dis = min(msg)
            if self._nodeValue[vertex] > min_dis:
                updated = True
                self._nodeValue[vertex] = min_dis
                for neighbor in self._graph[vertex]:
                    send_to_id = self.node_belong_to(neighbor)
                    
                    if neighbor not in self._msgout[send_to_id]:
                        self._msgout[send_to_id][neighbor] = [min_dis + 1]
                    else:
                        self._msgout[send_to_id][neighbor].append(min_dis + 1)

        print('calculation finished')
        for send_to_id in self._msgout.keys():
            if send_to_id == self.worker_id:
                self.store_to_msgin(self._msgout[send_to_id])
                continue
            handle = get_tcp_client_handle(self.peers[send_to_id])
            handle.sava_transfer_data(self._msgout[send_to_id])
        self.reach_barrier(updated)
        print('send finished')

    def reach_barrier(self, updated):
        handle = get_tcp_client_handle(self.master)
        handle.finish_iteration(self.worker_id, updated)
