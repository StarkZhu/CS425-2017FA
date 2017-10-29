import time
import random

class SDFS_Master():
    def __init__(self):
        # key - filename
        # value - [[node1, node2, node3], version, timestamp]
        self.file_metadata = {}
        self.member_list = []

    def update_member_list(self, member_list):
        self.member_list = member_list
        self.check_all_replica()

    def check_all_replica(self):
        # TODO: implement this function
        return False

    def handle_put_request(self, filename):
        # print("inside handle_put_request: ", filename)
        self.update_timestamp(filename)
        self.init_replica_nodes(filename)
        self.file_metadata[filename][1] += 1
        result = { 'ips': self.file_metadata[filename][0], 
                'ver': self.file_metadata[filename][1] }
        # print("before finish handle_put_request: ", result)
        return result

    def init_replica_nodes(self, filename):
        # print("inside init_replica_nodes: ", filename)
        replicas = set(self.file_metadata[filename][0])
        # print(replicas)
        # print(self.member_list)
        while len(replicas) < 3:
            num = random.randint(0, len(self.member_list)-1)
            # print("random int is: ", num)
            ip = self.member_list[num][0]
            if ip not in replicas:
                # print("adding new replica to: ", ip)
                replicas.add(ip)
                self.file_metadata[filename][0].append(ip)

    def get_file_timestamp(self, filename):
        # print("inside get_file_timestamp: ", filename)
        if filename not in self.file_metadata:
            return -1
        return self.file_metadata[filename][2]

    def get_file_version(self, filename):
        if filename not in self.file_metadata:
            return -1
        return self.file_metadata[filename][1]
    
    '''
    def m_get_file_replica_list(self, filename):
        if filename not in self.file_metadata:
            return []   # empty list
        return self.file_metadata[0]
    '''

    def file_updated_recently(self, filename):
        # print("inside file_updated_recently: ", filename)
        if filename not in self.file_metadata:
            # print("about to return False")
            return False
        cur_time = time.time()
        last_update = self.get_file_timestamp(filename)
        return cur_time - last_update < 60

    def update_timestamp(self, filename):
        # print("inside update_timestamp: ", filename)
        if filename not in self.file_metadata:
            # print("creating value for ", filename)
            self.file_metadata[filename] = []
            self.file_metadata[filename].append([])
            self.file_metadata[filename].append(0)
            self.file_metadata[filename].append(0)
        self.file_metadata[filename][2] = time.time()
        # print(self.file_metadata[filename])




class SDFS_Slave():
    def __init__(self):
        self.local_files = {}

    def update_file_version(self, filename, version):
        self.local_files[filename] = version

    def put_file(self, filename, file_data, version):
        self.update_file_version(filename, version)
        f = open(filename, "wb")
        f.write(file_data)
        f.close()

    def get_file(self, filename):
        f = open(filename, "rb")
        file_data = f.read()
        f.close()
        return file_data, self.local_files[filename]

  
