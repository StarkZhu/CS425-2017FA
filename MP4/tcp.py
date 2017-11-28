from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from xml.sax.saxutils import escape
from socket import *
from utils import *
from sdfs import SDFS_Master
from sava import * 

import base64
from timeout import Timeout
import time
import subprocess

# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

# Create Server Object
server = SimpleXMLRPCServer(
            ("0.0.0.0", 8000),
            requestHandler=RequestHandler,
            logRequests=False,
            allow_none=True
)
server.register_introspection_functions()


class TCPServer():
    def __init__(self, slave, sdfs_master, logger):
        self._slave = slave
        self._logger = logger

        self._sdfs_master = sdfs_master
        # self._sdfs = sdfs
        self.sava_worker = None
        self.sava_master = None

    def ping(self):
        self._logger.info("ping")
        return "pong"

    def dgrep(self, path, regEx):
        command = 'cat ' + path + ' | grep ' + regEx
        self._logger.debug(command)
        
        text = subprocess.run(
            command, 
            stdout=subprocess.PIPE, 
            shell=True,
            encoding='utf-8', 
            errors='replace',
        ).stdout

        self._logger.debug(text)

        return base64.b64encode(text.encode('utf-8'))

    def generate_log(self, server_id):
        # supposing we are generating 102 line each server
        file = open('machine.{}.log'.format(server_id), 'w')
        
        # unique pattern per machine 
        file.write("This is machine-{}.\n".format(server_id))

        # frequent pattern 
        for i in range(0, 80):
            hash = random.getrandbits(128)
            file.write("frequent_pattern_{%016x}\n" % hash)

        # somewhat frequent pattern 
        for i in range(0, 20):
            hash = random.getrandbits(128)
            file.write("somewhat_{%016x}\n" % hash)

        # only in even machines
        if server_id % 2 == 0:
            file.write("EVEN\n")

        return 0

# ------------------------- distributed grep


# ------------------------- sdfs master functions

    def put_file_info(self, sdfsfilename, requester_ip):
        # check when last time updated this file
        start_time = time.time()
        if self._sdfs_master.file_updated_recently(sdfsfilename):
            self._logger.info('Write conflict detected: {}s'.format(
                time.time() - start_time,
            ))
            # ask for confirmation 
            try:
                with Timeout(30):
                    if requester_ip == getfqdn():
                        command = input('This file was recently updated, are you sure you want to proceed? (yes/no) ')
                        if command != 'yes':
                            return False
                    else:
                        requester_handle = get_tcp_client_handle(requester_ip)
                        if not requester_handle.confirmation_handler():
                            return False

            except Timeout.Timeout:
                # abadon operation
                return False
        
        # get 3 target node and send to them the file
        put_info = self._sdfs_master.handle_put_request(sdfsfilename)
        # return ack to requester
        return put_info

    def get_file_info(self, sdfsfilename):
        # look up who has the file
        # send reqeust to get the file
        return self._sdfs_master.get_file_replica_list(sdfsfilename)

    def delete_file_info(self, sdfsfilename):
        # look up who has file 
        # send request to delete the file
        return self._sdfs_master.delete_file_info(sdfsfilename)

# ------------------------- sdfs master functions

# ------------------------- sdfs client functions
    def confirmation_handler(self):
        '''
        ask confirmation from client
        '''
        try:
            with Timeout(30):
                command = input('This file was recently updated, are you sure you want to proceed? (yes/no) ')

                if command == 'yes':
                    return True
                else:
                    return False

        except Timeout.Timeout:
            return False
    
    def remote_put_to_replica(self, target_ip, local_filename, sdfs_filename, ver):
        '''
        send file to replica
        '''
        self._logger.info('Got Remote Put Request {} {} {}'.format(
            target_ip,
            sdfs_filename,
            ver
        ))
        self._slave.put_to_replica(target_ip, local_filename, sdfs_filename, ver)
        return True

# ------------------------- sdfs client functions

# ------------------------- sdfs slave functions
    def put_file_data(self, sdfs_filename, file, ver, requester_ip):
        '''
        save file to sdfs
        '''
        self._slave.put_file_data(sdfs_filename, file, ver, requester_ip)
        return True

    def get_file_data(self, sdfs_filename, ver):
        '''
        read data from sdfs
        '''
        return self._slave.get_file_data(sdfs_filename, ver)

    def delete_file_data(self, sdfs_filename):
        '''
        delete sdfs file
        '''
        self._slave.delete_file_data(sdfs_filename)
        return True

    def vote(self, ip):
        # look up who has file 
        # send request to delete the file
        return self._slave.receive_vote(ip)

    def assign_new_master(self, ip):
        '''
        assgin new master and return a list of file holding
        '''
        msg = self._slave.assign_new_master(ip)
        # print(msg)
        return msg

    def update_file_version(self, filename, ver):
        # look up who has file 
        # send request to delete the file
        self._slave.update_file_version(filename, ver)
        return True

        
# ------------------------- sdfs slave functions

# ------------------------- start sava functions
    def set_sava_worker(self, sava_worker):
        self.sava_worker = sava_worker

    def set_sava_master(self, sava_master):
        self.sava_master = sava_master

    def sava_transfer_data(self, msg):
        self.sava_worker.store_to_msgin(msg)
        return True

    def finish_iteration(self, worker_id, updated):
        self.sava_master.finish_iteration(worker_id, updated)
        return True

    def init_sava_master(self, source_node, members):
        if self.sava_master is None:
            self.set_sava_master(SavaMaster())
        self.sava_master.initialize(source_node, members)
        return True

    def init_sava_worker(self, worker_id, source_node, workers):
        print('tcp worker init requested')
        self.sava_worker.initialize(worker_id, source_node, workers)
        return True

    def next_iter(self):
        self.sava_worker.process()
        return True

    def finish_work(self):
        self.sava_worker.save_result()
        return True
# ------------------------- end sava functions



