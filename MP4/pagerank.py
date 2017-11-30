# from pagerank import PageRank
import time

class Application():
    def __init__(self, args):
        self.max_iter = int(args[1]) + 1
        self.initial_value = 1

    def process(self, last_round_msgin, nodeValue, graph):
        print('start processing')

        cur_time = time.time()
        for vertex, msg in last_round_msgin.items():
            ranks = sum(msg)
            nodeValue[vertex] = 0.85*ranks + 0.15
        print('G and A takes {}s'.format(time.time() - cur_time))
        cur_time = time.time()

        nodeContrib = {}
        for vertex in nodeValue.keys():
            nodeContrib[vertex] = nodeValue[vertex]*1.0/len(graph[vertex])
        return nodeValue, nodeContrib

    def init_values(self):
        return [{}, 1]

    def calc_msg_out(self, prev_val, cur_val):
        return prev_val + cur_val

