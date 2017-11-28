import time
import math
import random
import os
import getpass
import traceback

from operator import itemgetter
from threading import Thread
from threading import Lock
from sdfs import SDFS_Slave

from socket import *
from global_vars import *
from utils import *

class Slave():
    
    def __init__(self, logger, sdfs_master):
        #self._udp_server = None
        # [ip, hb_count, local_timestamp]
        self._member_list = []
        self._neighbors = []
        self._recent_removed = []
        self._vote_num = 0
        self._voting = False
        self._voters = {}
        self._alive = False

        self._my_socket = socket(AF_INET, SOCK_DGRAM)
        self._sdfs = SDFS_Slave()
        self._sdfs_master = sdfs_master
        self._logger = logger
        self._master = INTRODUCER
        self._work_in_progress = {}
        self._lock = Lock()
        self._hb_lock = Lock()


    def is_alive(self):
        return self._alive and len(self._member_list) >= MACHINE_NUM

    def init_join(self):
        self._alive = True
        while len(self._member_list) < MACHINE_NUM or self._member_list[0][1] < 1:
            self.send_udp_msg(self._master, [('join', getfqdn())])
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

            self._hb_lock.acquire()
            for index in self._neighbors:
                if index >= len(self._member_list):
                    continue
                ip = self._member_list[index][0]

                if random.randint(0, 99) < UDP_LOST_RATE or ip in has_sent:
                    continue
                self.send_udp_msg(ip, self._member_list)
                has_sent.add(ip)
            self._hb_lock.release()

    def update_heartbeat_count(self):
        # find self index
        idx = self.find_membership_idx(getfqdn())
        self._member_list[idx] = (
            self._member_list[idx][0], 
            self._member_list[idx][1]+1, #increment heartbeat
            time.time(),
        )

    def maintenance_func(self):
        '''
        run in a thread, in charge of all maintenance work, including:
        failure detection, clean removed nodes list, update member list, revote leader
        '''
        while True:
            time.sleep(HEARTBEAT_PERIOD)
            if not self.is_alive():
                continue
            self.detect_failure()
            self.clean_removed_list()
            self._sdfs_master.update_member_list(self._member_list)
            if self._master not in [x[0] for x in self._member_list]:
                self.revote_master()

    def clean_removed_list(self):
        '''
        remove a machine from the recent_removed list after certain amount of time
        '''
        cur_time = time.time()
        for member in self._recent_removed:
            if member[2] < cur_time - REJOIN_COOLDOWN:
               self._recent_removed.remove(member)

    
    def detect_failure(self):
        '''
        check member's timestampe to detect failed nodes, enable file replication if needed
        '''
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
                #print('FAILURE DETECTED @ {}'.format(member))
                index = self.find_membership_idx(member[0])
                self._member_list.pop(index)

        self.update_neighbors()
        self._sdfs_master.update_member_list(self._member_list)
        if need_update:
            worker = Thread(target = self.fail_recover)
            worker.run()

    def fail_recover(self):
        '''
        check all existing files, if alive replica is less than 3, ask a good node to PUT the file to a new node, aka replicate the file, until 3 replica is reached
        '''
        time.sleep(HEARTBEAT_PERIOD*8)
        self._sdfs_master.update_member_list(self._member_list)
        start_time = time.time()

        update_meta = self._sdfs_master.update_metadata(self._member_list)
        if len(update_meta) == 0: return

        #self._logger.info(update_meta)
        for filename, meta in update_meta.items():
            good_node, ver, new_nodes = meta
            # self._logger.info('Contacting %s' % good_node)

            good_node_handle = get_tcp_client_handle(good_node)
            for ip in new_nodes:
                self._logger.info('Reparing {}@{}'.format(filename,ip))
                if good_node == getfqdn():
                    self.put_to_replica(ip, SDFS_PREFIX + filename, filename, ver)
                else:
                    good_node_handle.remote_put_to_replica(ip, SDFS_PREFIX + filename, filename, ver)
        self._logger.info("Repair done [{}s]".format(time.time() - start_time))

    def update_neighbors(self):
        self._hb_lock.acquire()
        idx = self.find_membership_idx(getfqdn())
        member_list_len = len(self._member_list)

        self._neighbors = [
            (idx-2)%member_list_len,
            (idx-1)%member_list_len,
            (idx+1)%member_list_len,
            (idx+2)%member_list_len,
        ]
        self._hb_lock.release()
        
        self._logger.debug(self._neighbors)

    def merge_member_list(self, remote_member_list):
        '''
        when receive heartbeat messages, merge the received member_list and local one, update information regarding offline machines and/or new join machines
        '''
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
        '''
        used by introducer/leader only
        handle all join request, enable entire system when enough machine has joined
        '''
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
        '''
        used by introducer/leader
        '''
        self._logger.info("Time[{}]: {} volunterally left".format(time.time(), leaver_ip))
        
        leaver_index = self.find_membership_idx(leaver_ip)
        self._recent_removed.append(self._member_list[leaver_index])
        self._member_list.pop(leaver_index)
        self.update_neighbors()

    def find_membership_idx(self, ip):
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

    # def put_file_data(self, sdfs_filename, input_file, ver, requester_ip):
    #     self._logger.info('put_file {} {}'.format(sdfs_filename, ver))
    #     file_data = input_file.data
    #     self._sdfs.put_file(sdfs_filename, file_data, ver)
    #     self._logger.debug('Prep to return to requester {}'.format(requester_ip))
    #     return True

    def init_work(self, sdfs_filename):
        '''
        used to count quorum
        '''
        self._lock.acquire()
        self._work_in_progress[sdfs_filename] = []
        self._lock.release()

    def put(self, local_filename, sdfs_filename):
        '''
        put a file into SDFS
        '''
        start_time = time.time()
        # contact master to register meta info, receive 3 slave names to transfer file data
        master_handle = get_tcp_client_handle(self._master)
        put_info = master_handle.put_file_info(sdfs_filename, getfqdn())
        #self._logger.info(put_info)

        if not put_info:
            self._logger.info('Put operation aborted.')
            return False

        self.init_work(sdfs_filename)
        ips = put_info['ips']
        ver = put_info['ver']
        # client transfer file data through network to 3 destination nodes
        for ip in ips:
            p = Thread(target=self.put_to_replica, args=(ip, local_filename, sdfs_filename, ver))
            p.start()

        # return to user immediately when quorum is satisfied
        while True:
            time.sleep(HEARTBEAT_PERIOD)
            if len(self._work_in_progress[sdfs_filename]) >= self.calc_quorum(len(ips)):
                self._logger.debug('quorum count: {}'.format(len(self._work_in_progress[sdfs_filename])))
                self._logger.info('Put %s@%d Done. [%fs]' % (
                    sdfs_filename, 
                    ver,
                    time.time() - start_time
                ))
                self._lock.acquire()
                del self._work_in_progress[sdfs_filename]
                self._lock.release()
                return True

    def put_to_replica(self, target_ip, local_filename, sdfs_filename, ver):
        '''
        transfer file data to a destination node
        may also be used to repair files in failure detection
        '''
        replica_handle = get_tcp_client_handle(target_ip)
        try:
            
            #with open(local_filename, 'rb') as f:
                #data = f.read()
                #replica_handle.put_file_data(sdfs_filename, xmlrpc.client.Binary(f.read()), ver, getfqdn())
            
            cmd = 'scp {} {}@{}:{}'.format(
                local_filename,
                getpass.getuser(),
                target_ip,
                MP_DIR + SDFS_PREFIX + sdfs_filename
            )
            os.system(cmd)
            replica_handle.update_file_version(sdfs_filename, ver)
            self._logger.debug("put {} to {} done".format(sdfs_filename, target_ip))
        
        except:
            traceback.print_exc()
            self._logger.info("local file {} doesn't exist".format(local_filename))
        
        if not local_filename.startswith(SDFS_PREFIX):
            self.work_done(sdfs_filename, 1)

    def work_done(self, sdfs_filename, data):
        self._lock.acquire()
        if sdfs_filename in self._work_in_progress:
            self._work_in_progress[sdfs_filename].append(data)
        self._lock.release()

    def get_from_replica(self, target_ip, sdfs_filename, ver):
        replica_handle = get_tcp_client_handle(target_ip)
        file_data = replica_handle.get_file_data(sdfs_filename, ver)
        self.work_done(sdfs_filename, file_data)
        return file_data
    
    def get_file_data(self, sdfs_filename, ver):
        local_ver = self._sdfs.get_file_version(sdfs_filename)
        if local_ver < ver:
            # init repair
            p = Thread(target=self.get, args=(sdfs_filename, SDFS_PREFIX + sdfs_filename))
            p.start()
        return {'ver': local_ver,
                'ip': getfqdn()}
    
    def get(self, sdfs_filename, local_filename):
        start_time = time.time()
        # contact master for meta info, know who has whose replica
        self._logger.info('contacting master for metadata {}'.format(sdfs_filename))
        master_handle = get_tcp_client_handle(self._master)
        replica_list = master_handle.get_file_info(sdfs_filename)

        if len(replica_list) == 0:
            self._logger.info('NO FILE NAMED {} FOUND.'.format(sdfs_filename))
            return False
        ips = replica_list[0]
        ver = replica_list[1]
        self.init_work(sdfs_filename)

        # get files from all nodes that have the replica, ask for correct/latest version
        for ip in ips:
            p = Thread(target=self.get_from_replica, args=(ip, sdfs_filename, ver))
            p.start()

        while True:
            time.sleep(HEARTBEAT_PERIOD)
            if len(self._work_in_progress[sdfs_filename]) >= self.calc_quorum(len(ips)):
                self._logger.debug('quorum count: {}'.format(len(self._work_in_progress[sdfs_filename])))
                break
        # but return to user immediate when just 1 file transfer is completed
        self._lock.acquire()
        for file_meta in self._work_in_progress[sdfs_filename]:
            local_ver = file_meta['ver']
            if local_ver == ver or len(self._work_in_progress[sdfs_filename]) == 1:
                cmd = 'scp {}@{}:{} {}'.format(
                    getpass.getuser(),
                    file_meta['ip'],
                    MP_DIR + SDFS_PREFIX + sdfs_filename,
                    local_filename
                )
                os.system(cmd)
                break;
                #with open(local_filename, 'wb') as f:
                #    f.write(file_data.data)
        del self._work_in_progress[sdfs_filename]
        self._lock.release()

        if local_filename.startswith(SDFS_PREFIX):
            self._sdfs.update_file_version(sdfs_filename, ver)
            self._logger.debug('repair file {} done'.format(sdfs_filename))
        else:
            self._logger.info('write to local file {}'.format(local_filename))
            self._logger.info("Get done [{}s]".format(time.time() - start_time))

    def ls(self, sdfs_filename):
        master_handle = get_tcp_client_handle(self._master)
        replica_list = master_handle.get_file_info(sdfs_filename)

        if len(replica_list) == 0:
            self._logger.info('No Such File.')
            return 

        ips = replica_list[0]
        ver = replica_list[1]
        self._logger.info('File: {} Ver:{}'.format(sdfs_filename,ver))
        for i, ip in enumerate(ips):
            self._logger.info('Replica {}: {}'.format(i,ip,))

    def store(self):
        files = self._sdfs.ls_file()
        for k,v in files.items():
            self._logger.info('File: {} Ver: {}'.format(k,v))

    def delete(self, sdfs_filename):
        '''
        get meta info from master, then contact 3 replica nodes and ask them to delete
        '''
        master_handle = get_tcp_client_handle(self._master)
        old_nodes = master_handle.delete_file_info(sdfs_filename)
        for node in old_nodes:
            if node == getfqdn():
                self._sdfs.delete_file_data(sdfs_filename)
                continue
            node_handle = get_tcp_client_handle(node)
            node_handle.delete_file_data(sdfs_filename)
        self._logger.info("deletion is done for {}".format(sdfs_filename))

    def delete_file_data(self, sdfs_filename):
        self._sdfs.delete_file_data(sdfs_filename)
        self._logger.info("file is deleted: {}".format(sdfs_filename))

    def revote_master(self):
        '''
        vote for the machine with lowest number
        '''

        if not self._voting:
            self._vote_num = 0
            self._voters = {}
            self._voting = True
        if getfqdn() == self._member_list[0][0]:
            self._vote_num += 1
            return
        candidate_handle = get_tcp_client_handle(self._member_list[0][0])
        candidate_handle.vote(getfqdn())

    def calc_quorum(self, num):
        return math.ceil((num+1)/2)

    def receive_vote(self, voter):
        '''
        potential lead receive votes and count till quorum, then declare as new leader and notify all other machines
        '''
        #print("receive_vote is called")
        if not self._voting:
            self._voters = {}
            self._vote_num = 0
            self._voting = True
        if voter not in self._voters:
            self._voters[voter] = 1
            self._vote_num += 1
        #print("current vote num: {}".format(self._vote_num))
        #print("{}".format(self._voters))
        if self._master != getfqdn() and self._vote_num > len(self._member_list) / 2:
            self._master = getfqdn()
            self._logger.info("I am voted to be the new master")
            p = Thread(target=self.rebuild_file_meta)
            p.start()

    def rebuild_file_meta(self):
        '''
        ask for file meta info from all machines
        collect file info and build the metadata table, to be able to answer all file queries
        '''
        #print("rebuilding")
        time.sleep(HEARTBEAT_PERIOD*2)
        tmp_file_meta = {}
        for member in self._member_list:
            member_files = {}
            if member[0] == getfqdn():
                member_files = self._sdfs.ls_file()
            else:
                member_handle = get_tcp_client_handle(member[0])
                member_files = member_handle.assign_new_master(getfqdn())
            #print("[{}] member_files: {}".format(member[0], member_files))
            for filename, ver in member_files.items():
                if filename not in tmp_file_meta:
                    tmp_file_meta[filename] = []
                tmp_file_meta[filename].append([member[0], ver])
        #print("tmp_file_meta: {}".format(tmp_file_meta))
        for filename, file_list in tmp_file_meta.items():
            sorted(file_list, key=lambda x : x[1])
            value = []
            value.append([x[0] for x in file_list][0:min(len(file_list), 3)])
            value.append(file_list[0][1])
            value.append(time.time())
            self._sdfs_master.put_file_metadata(filename, value)
        self._logger.info("SDFS file metadata has been rebuilt")
        self._voting = False
        self._voters = {}
        
        p = Thread(target=self.fail_recover)
        p.start()

    def assign_new_master(self, new_master):
        self._master = new_master
        self._voting = False
        self._logger.info("accepting new matser: {}".format(new_master))
        return self._sdfs.ls_file()

    def update_file_version(self, filename, ver):
        self._sdfs.update_file_version(filename, ver)

    def submit_job(self, source_node):
        master_handle = get_tcp_client_handle(self._master)
        master_handle.init_sava_master(source_node, self._member_list)

    def run(self):
        my_thread = Thread(target=self.send_heartbeat)
        maintenance_thread = Thread(target=self.maintenance_func)
        my_thread.start()
        maintenance_thread.start()
