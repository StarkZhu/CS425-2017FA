from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from xml.sax.saxutils import escape
from socket import *
from sdfs import SDFS_Master

import base64
from timeout import Timeout


# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)


server = SimpleXMLRPCServer(
            ("0.0.0.0", 8000),
            requestHandler=RequestHandler,
            logRequests=False
)
server.register_introspection_functions()


class TCPServer():
    def __init__(self, slave, sdfs_master, logger):
        self._slave = slave
        self._logger = logger

        self._sdfs_master = sdfs_master
        # self._sdfs = sdfs

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
        if self._sdfs_master.file_updated_recently(sdfsfilename):
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
        return False

# ------------------------- sdfs master functions

# ------------------------- sdfs client functions
    def confirmation_handler(self):
        try:
            with Timeout(30):
                return True
                command = input('This file was recently updated, are you sure you want to proceed? (yes/no) ')

                if command == 'yes':
                    return True
                else:
                    return False

        except Timeout.Timeout:
            return False
    
    def remote_put_to_replica(self, target_ip, local_filename, sdfs_filename, ver):
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
        self._slave.put_file_data(sdfs_filename, file, ver, requester_ip)
        return True

    def get_file_data(self, sdfs_filename, ver):
        self._logger.info('get_file_data {} {}'.format(
            sdfs_filename, 
            ver,
        ))

        local_ver, file_data = self._sdfs.get_file(sdfs_filename, ver)

        if local_ver < ver:
            # init repair 
            p = Thread(target=self._slave.get, args=(sdfs_filename, SDFS_PREFIX + SDFS_PREFIX))
            p.start()


        return {
            'ver': local_ver,
            'file_data': xmlrpc.client.Binary(file_data)
        }


# ------------------------- sdfs slave functions



