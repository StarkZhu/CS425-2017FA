from global_vars import *
from utils import *
from threading import Lock
from threading import Thread
from collections import defaultdict
from collections import deque
from importlib import reload

import time
import sys
import operator

class SavaMaster():
    def __init__(self):
        self._workers = {}
        self.finish_cnt = 0
        self.has_update = False
        self._iter_cnt = 0

        self.application = None
        self._slave = None

    def initialize(self, args, members, slave):
        self._iter_cnt = 0
        self._slave = slave
        self.set_workers(members)
        p = Thread(target=self.init_work, args=[args])
        p.start()

        self.start_time = time.time()
        print('Master Initialize Done')

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
            self._iter_cnt += 1
            self.finish_cnt = 0

            # call start iteration
            if self.has_update:
                p = Thread(target=self.proceed_next_iter)
                p.start()
            else:
                p = Thread(target=self.finish)
                p.start()
            self.has_update = False


        '''
        if self.max_iter == -1 and self._iter_cnt ==0:            
            p = Thread(target=self.get_max_iter, args=[worker_id])
            p.start()
        elif self.finish_cnt == len(self._workers):
            p = Thread(target=self.init_next_iter)
            p.start()
        elif self.finish_cnt == len(self._workers)*2:

            # work is complete
            print('current iteration %d' % self._iter_cnt)
            self.finish_cnt = 0
            self._iter_cnt += 1
            
            # call start iteration
            print('max_iter:',  self.max_iter)
            if (self._iter_cnt < 3 or (self.has_update and self._iter_cnt < self.max_iter)):
                p = Thread(target=self.proceed_next_iter)
                p.start()
            else:
                p = Thread(target=self.finish)
                p.start()
            self.has_update = False
        '''

    def get_max_iter(self, worker_id):
        handle = get_tcp_client_handle(self._workers[worker_id])
        self.max_iter = handle.get_max_iter()
            
    def finish(self):
        print('taking time {}s'.format(time.time() - self.start_time))
        for worker_ip in self._workers.values():
            handle = get_tcp_client_handle(worker_ip)
            handle.finish_work()

        p = Thread(target=self.get_final_result)
        p.start()


    def get_final_result(self):
        # open a `thread` to gather result 
        result = {}
        for worker_id in self._workers.keys():
            filename = '%s_sava_output' % worker_id
            self._slave.get(filename, filename)
            with open(filename) as fin:
                for line in fin:
                    splits = line.strip().split(' ')
                    result[splits[0]] = float(splits[1])

        sorted_result = sorted(result.items(), reverse=True, key=operator.itemgetter(1))
        with open('agg_result.txt', 'w') as fout:
            for i, e in enumerate(sorted_result):
                if i >= 25:
                    break
                fout.write('{} {}\n'.format(e[0], e[1]))

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
            p = Thread(target=self.init_worker_thread, args=[worker_id, worker_ip, args])
            p.start()

    def init_worker_thread(self, worker_id, worker_ip, args):
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

        self.application = None
        self.last_round_msgin = {}
        self.partition = {}

        self._max_iter = -1
        self._iter_cnt = 0

    def initialize_thread(self, worker_id, args, workers, slave):
        self._slave = slave

        self.worker_id = worker_id
        self.set_peers(workers)

        print('Worker %s initializing' % self.worker_id)
        self._slave.get(SAVA_APP_PY, SAVA_APP_PY)
        import application

        reload(application)
        self.application = application.Application(args)
        self._max_iter = self.application.max_iter
        initial_values = self.application.init_values();
        self.load_graph(initial_values[0], initial_values[1])

        print('worker init done')

    def initialize(self, worker_id, args, workers, slave):
        p = Thread(target=self.initialize_thread, args=[worker_id, args, workers, slave])
        p.start()

    def get_max_iter(self):
        return self.application.max_iter
        
    def set_master(self, master):
        self.master = master

    def set_worker_id(self, id):
        self.worker_id = id

    def set_peers(self, peers):
        # id:ip
        self.peers = peers

    def load_graph(self, init_value, default_value):
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

        # initialize values 
        for node in self._graph.keys():
            if (self.node_belong_to(node) == self.worker_id):
                if node == '1':
                    print("I have source node")
                    print(init_value)
                if node in init_value:
                    self._nodeValue[node] = init_value[node]
                    print('node {} is initialized to be {}'.format(node, init_value[node]))
                else:
                    self._nodeValue[node] = default_value

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

    def store_to_msgin_thread(self, msg):
        self._lock.acquire()
        for node in msg.keys():
            self._msgin[node] += msg[node]
        self._lock.release()

    def store_to_msgin(self, msg):
        p = Thread(target=self.store_to_msgin_thread, args=[msg])
        p.start()

    def save_result(self):
        sorted_result = sorted(
            self._nodeValue.items(), 
            reverse=True, 
            key=operator.itemgetter(1)
        )

        filename = '%s_sava_output' % self.worker_id
        with open(filename, 'w+') as fout:
            # for key, value in self._nodeValue.items():
            cnt = 0
            for key, value in sorted_result:
                if cnt >= 25:
                    break
                cnt += 1
                fout.write('{} {}\n'.format(key, value))

        self._slave.put(filename, filename)

    def process(self):
        p = Thread(target=self.process_thead)
        p.start()
    
    def process_thead(self):
        
        nodeValue, nodeContrib = self.application.process(
            self.last_round_msgin, 
            self._nodeValue, 
            self._graph
        )

        for vertex in nodeContrib.keys():
            for neighbor in self._graph[vertex]:
                send_to_id = self.node_belong_to(neighbor)



                if neighbor not in self._msgout[send_to_id]:
                    self._msgout[send_to_id][neighbor] = [nodeContrib[vertex]]
                else:
                    self._msgout[send_to_id][neighbor][0] = self.application.calc_msg_out(
                        self._msgout[send_to_id][neighbor][0], 
                        nodeContrib[vertex],
                    )

        cur_time = time.time()

        print('calculation finished')
        threads = []
        for send_to_id in self._msgout.keys():
            p = Thread(target=self.send_msg_thread, args=[send_to_id])
            p.start()
            threads.append(p)

        for p in threads:
            p.join()

        print('send finished, takes {}s'.format(time.time() - cur_time))     
        cur_time = time.time()

        self._iter_cnt += 1

        if (self._iter_cnt > 3 and len(nodeContrib) <= 1) or self._iter_cnt >= self._max_iter:
            self.reach_barrier(False)
        else:
            self.reach_barrier(True)
        print('wait for barrier takes{}s'.format(time.time() - cur_time))


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
        self.reach_barrier(False)    

    def send_msg_thread(self, send_to_id):
        print('msg send to {} size: {}'.format(send_to_id, sys.getsizeof(str(self._msgout[send_to_id]))))
        if send_to_id == self.worker_id:
            self.store_to_msgin(self._msgout[send_to_id])
            return
        handle = get_tcp_client_handle(self.peers[send_to_id])
        handle.sava_transfer_data(self._msgout[send_to_id])

    def reach_barrier(self, updated):
        handle = get_tcp_client_handle(self.master)
        handle.finish_iteration(self.worker_id, updated)
