# from pagerank import PageRank
import time

class Application():
    def __init__(self, args):
        self.source_node = args[1]
        self.initial_value = float('inf')
        self.max_iter = float('inf')

    def process(self, last_round_msgin, nodeValue, graph):
        print('start processing')
        updated = False
        nodeContrib = {self.source_node : 1}
        for vertex, msg in last_round_msgin.items():
            min_dis = min(msg)
            if nodeValue[vertex] > min_dis:
                updated = True
                nodeValue[vertex] = min_dis
                nodeContrib[vertex] = min_dis + 1
        return nodeValue, nodeContrib
        #
        #if updated:
        #    return nodeValue, nodeContrib
        #else:
        #    return nodeValue, None

    def init_values(self):
        return [{self.source_node : 0}, float('inf')]

    def calc_msg_out(self, prev_val, cur_val):
        return min(prev_val, cur_val)