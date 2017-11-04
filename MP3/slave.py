import time
import threading
import random
from operator import itemgetter

from socket import *
from global_vars import *
from utils import *

class Slave():
    
    def __init__(self, logger):
        #self._udp_server = None
        self._member_list = []
        self._neighbors = []
        self._recent_removed = []

        self._alive = False

        self._my_socket = socket(AF_INET, SOCK_DGRAM)
        self._sdfs = None
        self._logger = logger



    def is_alive(self):
        return self._alive and len(self._member_list) >= MACHINE_NUM

    def init_join(self):
        self._alive = True
        while len(self._member_list) < MACHINE_NUM or self._member_list[0][1] < 1:
            self.send_udp_msg(INTRODUCER, [('join', getfqdn())])
            time.sleep(HEARTBEAT_PERIOD)

    def send_udp_msg(self, target, obj):
        addr = (target, UDP_PORT_NUM)
        msg = encode_obj(obj)
        self._my_socket.sendto(msg, addr)

    def send_heartbeat(self):
        while True:
            time.sleep(HEARTBEAT_PERIOD)

            if not self.is_alive():
                continue

            self.update_heartbeat_count()
            has_sent = set()
            for index in self._neighbors:
                ip = self._member_list[index][0]

                if random.randint(0, 99) < UDP_LOST_RATE or ip in has_sent:
                    continue
                
                self.send_udp_msg(ip, self._member_list)
                has_sent.add(ip)

    def update_heartbeat_count(self):
        # find self index
        idx = self.find_membership_idx(getfqdn())
        self._member_list[idx] = (
            self._member_list[idx][0], 
            self._member_list[idx][1]+1, #increment heartbeat
            time.time(),
        )

    def maintenance_func(self):
        while True:
            time.sleep(HEARTBEAT_PERIOD)
            if not self.is_alive():
                continue
            self.detect_failure()
            self.clean_removed_list()

    def clean_removed_list(self):
        '''
        remove a machine from the recent_removed list after certain amount of time
        '''
        cur_time = time.time()
        for member in self._recent_removed:
            if member[2] < cur_time - REJOIN_COOLDOWN:
               self._recent_removed.remove(member)

    
    def detect_failure(self):
        # go through member_list to check if any machine is offline
        cur_time = time.time()
        need_update = False

        for member in self._member_list:
            if member[0] == getfqdn(): continue
            if member[1] == 0: continue
            if member[2] < cur_time - GRACE_PERIOD:
                need_update = True
                # remove offline machine from member_list but remember its last alive stats
                self._recent_removed.append(member)
                print('FAILURE DETECTED @ {}'.format(member))
                #print("curent m_list: {}".format(self._member_list))
                self._member_list.remove(member)
        self.update_neighbors()

        # if need_update:
        #   worker = Thread(target = fail_recover)
        #   worker.run()


    def update_neighbors(self):
        idx = self.find_membership_idx(getfqdn())
        member_list_len = len(self._member_list)

        self._neighbors = [
            (idx-2)%member_list_len,
            (idx-1)%member_list_len,
            (idx+1)%member_list_len,
            (idx+2)%member_list_len,
        ]
        self._logger.debug(self._neighbors)

    def merge_member_list(self, remote_member_list):
        # when receive heartbeat messages, merge the received member_list and local one, update information regarding offline machines and/or new join machines

        if not self._alive: return 

        j = 0
        my_len = len(self._member_list)
        list_len = len(remote_member_list)
        cur_time = time.time()
        for i in range(list_len):
            # my member_list has machines not appearing in others, skip them
            while (j < my_len and self._member_list[j][0] < remote_member_list[i][0]):
                j+=1
            # machine appears in both member_list, check heartbeat count to decide whether to update
            if (j < my_len and remote_member_list[i][0] == self._member_list[j][0]):
                if (remote_member_list[i][1] > self._member_list[j][1]):
                    # update timestampe and heartbeat count only when new count is larger
                    self._member_list[j] = (self._member_list[j][0], remote_member_list[i][1], cur_time)
                j+=1
            else:
                # my member_list is not displaying a machine that appears in others' list
                tmpList = [m[0] for m in self._recent_removed]
                domain_name = remote_member_list[i][0]
                if domain_name not in tmpList or self._recent_removed[tmpList.index(domain_name)][1] < remote_member_list[i][1]:
                    # only append it if it is not a machine recently detected to be offline
                    self._member_list.append(remote_member_list[i])
                    if domain_name not in tmpList:
                        self._logger.debug("Time[{}]: {} is joining.".format(time.time(), domain_name))

        self._member_list = sorted(self._member_list, key=itemgetter(0))
        self.update_neighbors()


    def handle_join_request(self, joiner_ip):
        if joiner_ip in [m[0] for m in self._member_list]: return

        self._logger.info("Time[{}]: {} is joining.".format(time.time(), joiner_ip))

        new_joiner = (joiner_ip, 0, time.time())
        self._member_list.append(new_joiner)
        self._member_list = sorted(self._member_list, key=itemgetter(0))

        # assumption that there is more than 5 machine at any given time 
        if(len(self._member_list) == MACHINE_NUM):
            # print(self._member_list)
            self.update_heartbeat_count()
            self.update_neighbors()
            # send member list to all machines
            for member in self._member_list:
                if member[0] == getfqdn():
                    continue
                self.send_udp_msg(member[0], self._member_list)

    def handle_leave_request(self, leaver_ip):
        self._logger.info("Time[{}]: {} volunterally left".format(time.time(), leaver_ip))
        
        leaver_index = self.find_membership_idx(leaver_ip)
        self._recent_removed.append(self._member_list[leaver_index])
        self._member_list.pop(leaver_index)
        self.update_neighbors()

    def find_membership_idx(self, ip):
        # print(self._member_list)
        return [m[0] for m in self._member_list].index(ip)

    def leave(self):
        self._alive = False

        reset_member_list = []
        for member in self._member_list:
            reset_member_list.append((member[0], 0, time.time()))
            if member[0] == getfqdn():
                continue
            self.send_udp_msg(member[0], [('leave', getfqdn())])
        self._member_list = reset_member_list

    def run(self):
        my_thread = threading.Thread(target=self.send_heartbeat)
        maintenance_thread = threading.Thread(target=self.maintenance_func)
        my_thread.start()
        maintenance_thread.start()
