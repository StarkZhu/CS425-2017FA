import time
import random
import os
from utils import *

SDFS_PREFIX = 'sdfs/'

class SDFS_Master():
    '''
    master node keeps meta info about all files in SDFS:
    where 3 replica are saved, latest version, last update time (local clock)
    '''
    def __init__(self):
        # key - filename
        # value - [[node1, node2, node3], version, timestamp]
        self.file_metadata = {}
        self.member_list = []
    
    def update_member_list(self, member_list):
        self.member_list = member_list

    def put_file_metadata(self, filename, value):
        self.file_metadata[filename] = value

    def update_metadata(self, member_list):
        to_replicate = {}
        alive = set([item[0] for item in member_list])
        for filename, value in self.file_metadata.items():
            bad = []
            good = []
            for node in self.file_metadata[filename][0]:
                if node not in alive:
                    bad.append(node)
                else:
                    good.append(node)
            if len(good) < 3:
                ver = self.file_metadata[filename][1]
                self.file_metadata[filename][0] = list(good)
                self.init_replica_nodes(filename)
                set1 = set(self.file_metadata[filename][0])
                set2 = set(good)
                new_nodes = list(set(self.file_metadata[filename][0]) - set(good))
                to_replicate[filename] = [good[0], ver, new_nodes]
        return to_replicate

    def handle_put_request(self, filename):
        self.update_timestamp(filename)
        self.init_replica_nodes(filename)
        self.file_metadata[filename][1] += 1
        result = { 'ips': self.file_metadata[filename][0], 
                'ver': self.file_metadata[filename][1] }
        return result

    def init_replica_nodes(self, filename):
        replicas = set(self.file_metadata[filename][0])
        while len(replicas) < 3:
            num = random.randint(0, len(self.member_list)-1)
            ip = self.member_list[num][0]
            if ip not in replicas:
                replicas.add(ip)
                self.file_metadata[filename][0].append(ip)

    def get_file_timestamp(self, filename):
        if filename not in self.file_metadata:
            return -1
        return self.file_metadata[filename][2]

    def get_file_version(self, filename):
        if filename not in self.file_metadata:
            return -1
        return self.file_metadata[filename][1]
    
    
    def get_file_replica_list(self, filename):
        if filename not in self.file_metadata:
            return []   # empty list
        return self.file_metadata[filename]
    

    def file_updated_recently(self, filename):
        if filename not in self.file_metadata:
            return False
        cur_time = time.time()
        last_update = self.get_file_timestamp(filename)
        return cur_time - last_update < 60

    def update_timestamp(self, filename):
        if filename not in self.file_metadata:
            self.file_metadata[filename] = []
            self.file_metadata[filename].append([])
            self.file_metadata[filename].append(0)
            self.file_metadata[filename].append(0)
        self.file_metadata[filename][2] = time.time()

    def delete_file_info(self, sdfs_filename):
        if sdfs_filename not in self.file_metadata:
            return []
        old_nodes = self.file_metadata[sdfs_filename][0]
        del self.file_metadata[sdfs_filename]
        return old_nodes


class SDFS_Slave():
    '''
    slave node in SDFS, only keeps info about files in current node/machine
    '''
    def __init__(self):
        # [filename, version]
        self.local_files = {}

    def update_file_version(self, filename, version):
        self.local_files[filename] = version

    def put_file(self, filename, file_data, version):
        self.update_file_version(filename, version)
        f = open(SDFS_PREFIX + filename, "wb")
        f.write(file_data)
        f.close()

    def get_file_version(self, filename):
        return self.local_files[filename]

    def get_file(self, filename, version):
        if (self.local_files[filename] != version):
            return self.local_files[filename], None
        f = open(SDFS_PREFIX + filename, "rb")
        file_data = f.read()
        f.close()
        return self.local_files[filename], file_data

    def ls_file(self):
        return self.local_files

    def delete_file_data(self, sdfs_filename):
        del self.local_files[sdfs_filename]
        os.remove(SDFS_PREFIX + sdfs_filename)
