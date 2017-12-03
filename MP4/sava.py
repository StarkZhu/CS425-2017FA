from global_vars import *
from utils import *
from threading import Lock
from threading import Thread
from collections import defaultdict
from collections import deque
from importlib import reload

import xmlrpc.client
import time
import sys
import operator
import zlib
import os
import getpass

class SavaMaster():
    def __init__(self, logger):
        self._workers = {}
        self.finish_cnt = 0
        self.has_update = False
        self._iter_cnt = 0

        self.application = None
        self._slave = None

        self.is_active = False
        self.args = None
        self._logger = logger

    def initialize(self, args, members, slave, is_active):
        self._iter_cnt = 0
        self.finish_cnt = 0
        self._slave = slave

        if args is not None:
            self.args = args

        self.job_id = self.args[2]
        self.set_workers(members)
        self.is_active = is_active

        if self.is_active:
            p = Thread(target=self.init_work, args=[self.args])
            p.start()

        self.start_time = time.time()
        self.iter_time = time.time()
        self._logger.info('Master Initialize Done')

    def calc_workers(self, members):
        workers = {}
        cnt = 0
        for m in members:
            mid = int(m[0].split('.')[0].split('-')[-1])
            if mid <= 3:
                continue

            workers[str(cnt)] = m[0]
            cnt += 1
        return workers

    def set_workers(self, members):
        self._workers = self.calc_workers(members)

    def finish_iteration(self, worker_id, updated, job_id):
        if job_id != self.job_id:
            self._logger.info('JOB ID NOT MATCHING')
            return

        self.finish_cnt += 1

        # self._logger.info('finish count: {} updated: {} '.format(self.finish_cnt, updated))
        # self._logger.info('worker %s finish iteration called' % worker_id)
        self.has_update = updated or self.has_update

        if self.finish_cnt == len(self._workers):
            # self._logger.info('active? ', self.is_active)
            self._logger.info('Aggregate and Scatter phase takes {}s'.format(time.time() - self.iter_time))
            if self.is_active:
                # self._logger.info('finish gather')
                p = Thread(target=self.init_next_iter)  #msgin done
                p.start()

            

        elif self.finish_cnt == len(self._workers)*2:
            # self._logger.info('active? ', self.is_active)
            # work is complete
            # self._logger.info('current iteration %d' % self._iter_cnt)
            self._logger.info('Iteration {} takes {}s'.format(self._iter_cnt, time.time() - self.iter_time))
            self._iter_cnt += 1

            # call start iteration
            if self.has_update:
                if self.is_active:
                    p = Thread(target=self.proceed_next_iter)   # calculation done
                    p.start()
            else:
                if self.is_active:
                    p = Thread(target=self.finish)
                    p.start()
            self.has_update = False
            self.finish_cnt = 0

            
            self.iter_time = time.time()

        # self._logger.info('END OF FINISH ITERATION')

    def get_max_iter(self, worker_id):
        handle = get_tcp_client_handle(self._workers[worker_id])
        self.max_iter = handle.get_max_iter()
            
    def finish(self):
        self._logger.info('taking time {}s'.format(time.time() - self.start_time))
        for worker_ip in self._workers.values():
            handle = get_tcp_client_handle(worker_ip)
            handle.finish_work()

        p = Thread(target=self.get_final_result)
        p.start()


    def get_final_result(self):
        # open a `thread` to gather result 
        result = {}
        threads = []
        for worker_id in self._workers.keys():
            filename = '%s_sava_output' % worker_id
            p = Thread(target=self._slave.get, args=[filename, filename])
            p.start()
            threads.append(p)

        for p in threads:
            p.join()

        for worker_id in self._workers.keys():
            filename = '%s_sava_output' % worker_id
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
            # self._logger.info('ask worker to gather', worker_ip)
            handle = get_tcp_client_handle(worker_ip)
            handle.init_next_iter()

    def proceed_next_iter(self):
        for worker_ip in self._workers.values():
            handle = get_tcp_client_handle(worker_ip)
            handle.proceed_next_iter()

    def init_work(self, args):
        self._logger.info('master start initializing')
        self._logger.info(self._workers)
        for worker_id, worker_ip in self._workers.items():
            p = Thread(target=self.init_worker_thread, args=[worker_id, worker_ip, args])
            p.start()

    def init_worker_thread(self, worker_id, worker_ip, args):
        handle = get_tcp_client_handle(worker_ip)
        handle.init_sava_worker(worker_id, args, self._workers, self.job_id)


class SavaWorker():
    def __init__(self, logging):
        self.worker_id = ''
        self.job_id = -1
        self._lock = Lock()
        self.peers = None
        self.application = None
        self._max_iter = -1
        self._iter_cnt = 0
        self.masters = [INTRODUCER, SAVA_STANDBY_MASTER]

        self._logger = logging

    def initialize_thread(self, worker_id, args, workers, slave, job_id):
        # map node_number to list
        self._msgin = defaultdict(list)
        self._msgout = defaultdict(list)
        self._graph = defaultdict(list)
        self._nodeValue = {}
        self.last_round_msgin = {}
        self.partition = {}

        self._slave = slave
        self.worker_id = worker_id
        self.job_id = job_id
        self.set_peers(workers)

        self._logger.info('Worker %s initializing' % self.worker_id)
        self._slave.get(SAVA_APP_PY, SAVA_APP_PY)
        import application

        reload(application)
        self.application = application.Application(args)
        self._max_iter = self.application.max_iter
        self._iter_cnt = 0
        initial_values = self.application.init_values();
        self.load_graph(initial_values[0], initial_values[1])

        self._logger.info('worker init done')

    def initialize(self, worker_id, args, workers, slave, job_id):
        p = Thread(target=self.initialize_thread, args=[worker_id, args, workers, slave, job_id])
        p.start()

    def get_max_iter(self):
        return self.application.max_iter
       
    '''
    def set_master(self, master):
        self.master = master
    '''

    def set_worker_id(self, id):
        self.worker_id = id

    def set_peers(self, peers):
        # id:ip
        self.peers = peers

    def load_graph(self, init_value, default_value):
        num_nodes = 0
        num_edges = 0

        self._logger.info('loading graph STARTED')
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

        # self._logger.info('loading graph DONE takes {}s'.format(time.time() - cur_time))
        # cur_time = time.time()
        self.bfs_partition()

        # initialize values 
        for node in self._graph.keys():
            if (self.node_belong_to(node) == self.worker_id):
                if node == '1':
                    self._logger.info("I have source node")
                    self._logger.info(init_value)
                if node in init_value:
                    self._nodeValue[node] = init_value[node]
                    self._logger.info('node {} is initialized to be {}'.format(node, init_value[node]))
                else:
                    self._nodeValue[node] = default_value

        
        self._logger.info('loading & partition DONE takes {}s'.format(time.time() - cur_time))

        self.reach_barrier(True)


    def bfs_partition(self):
        q = deque()
        sorted_nodes = []

        q.append('1')
        sorted_nodes.append('1')

        unvisited = set(self._graph.keys())
        while len(q) > 0:
            # self._logger.info(len(q), len(unvisited))
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
        self._logger.info('start to save partition')
        for i, vertex in enumerate(sorted_nodes):
            self.partition[vertex] = int(i * len(self.peers) / len(sorted_nodes))            
        # self._logger.info(len(self.partition))
    
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
        cur_time = time.time()

        nodeValue, nodeContrib = self.application.process(
            self.last_round_msgin, 
            self._nodeValue, 
            self._graph
        )

        self._logger.info('application specific calculation finished %fs' % (time.time() - cur_time))
        cur_time = time.time()

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

        self._logger.info('calculate msg to send finished %fs' % (time.time() - cur_time))
        cur_time = time.time()
        threads = []
        for send_to_id in self._msgout.keys():
            p = Thread(target=self.send_msg_thread, args=[send_to_id])
            p.start()
            threads.append(p)

        for p in threads:
            p.join()

        self._logger.info('send finished, takes {}s'.format(time.time() - cur_time))     
        # cur_time = time.time()

        self._iter_cnt += 1

        if (self._iter_cnt > 3 and len(nodeContrib) <= 1) or self._iter_cnt >= self._max_iter:
            self.reach_barrier(False)
        else:
            self.reach_barrier(True)
        # self._logger.info('wait for barrier takes{}s'.format(time.time() - cur_time))


    def gather(self):
        p = Thread(target=self.gather_msg)
        p.start()

    def gather_msg(self):
        # self._logger.info('start gather msg')
        self._msgout = defaultdict(dict)
        self._lock.acquire()
        self.last_round_msgin = self._msgin
        self._msgin = defaultdict(list)
        self._lock.release()
        self.reach_barrier(False)    

    def send_msg_thread(self, send_to_id):
        # self._logger.info('msg send to {} size: {}'.format(send_to_id, sys.getsizeof(str(self._msgout[send_to_id]))))
        
        if send_to_id == self.worker_id:
            self.store_to_msgin(self._msgout[send_to_id])
            return

        cur_time = time.time()
        compressed = zlib.compress(encode_obj(self._msgout[send_to_id]), level=-1)
        '''
        filename = 'msgto_%s.msg' % send_to_id
        with open(filename,'w') as fout:
            fout.write(str(self._msgout[send_to_id]))
        self._logger.info('write file takes ', time.time() - cur_time)
        cur_time = time.time()

        cmd = 'scp {} {}@{}:{}'.format(
                filename,
                getpass.getuser(),
                self.peers[send_to_id],
                MP_DIR + MSG_DIR + 'msgfrom_%s.msg' % self.worker_id
            )
        os.system(cmd)
        self._logger.info('send file takes ', time.time() - cur_time)
        '''
        # self._logger.info(send_to_id, ' compress takes %fs' % (time.time() - cur_time))
        handle = get_tcp_client_handle(self.peers[send_to_id])
        # handle.sava_transfer_data(self._msgout[send_to_id])
        handle.sava_transfer_data(xmlrpc.client.Binary(compressed).data)
        # handle.sava_transfer_data(self.worker_id)

    def reach_barrier(self, updated):
        tmp = list(self.masters)
        for master in tmp:
            # self._logger.info('send barrier to: ', master)
            try:
                handle = get_tcp_client_handle(master)
                handle.finish_iteration(self.worker_id, updated, self.job_id)
            except ConnectionRefusedError:
                self.masters.remove(master)
                self._logger.info('SAVA Master Failure detected ', master)
