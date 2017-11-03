from socket import *
from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from xml.sax.saxutils import escape
import time
from operator import itemgetter
import subprocess
import random
import base64
import xmlrpc.client
from threading import Thread
import logging
import signal
from sdfs import SDFS_Master, SDFS_Slave
from timeout import Timeout
from threading import Lock
import os
from multiprocessing import Process, Value, Array


# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

#member_list: (IP, heartbeat_count, timestamp)}
INTRODUCER = 'fa17-cs425-g29-01.cs.illinois.edu'
member_list = []
neighbors = []
recent_removed = []
false_positive_count = 0

UDP_LOST_RATE = 0
MACHINE_NUM = 3
ALIVE = False
HEARTBEAT_PERIOD = 0.7
GRACE_PERIOD = 3.1
REJOIN_COOLDOWN = 12.0
MSG_Q = []

sdfs = SDFS_Slave()
sdfs_master = SDFS_Master()
TRANSFER_IN_PROGRESS = {}
SDFS_PREFIX = 'sdfs/'

lock = Lock()

# ------------------------- distributed grep

def run_tcp_server():
    # Create server
    with SimpleXMLRPCServer(("0.0.0.0", 8000),
                            requestHandler=RequestHandler,
                            logRequests=False) as server:
        server.register_introspection_functions()

        def ping():
            logging.info("ping")
            return "pong"
        server.register_function(ping, 'ping')

        # Register a function under a different name
        def dgrep(path, regEx):
            command = 'cat ' + path + ' | grep ' + regEx
            logging.debug(command)
            
            text = subprocess.run(
                command, 
                stdout=subprocess.PIPE, 
                shell=True,
                encoding='utf-8', 
                errors='replace',
            ).stdout

            return base64.b64encode(text.encode('utf-8'))
        server.register_function(dgrep, 'dgrep')

        def generate_log(server_id):
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
        server.register_function(generate_log, 'glog')

# ------------------------- distributed grep


# ------------------------- sdfs master functions

        def put_file_info(sdfsfilename, requester_ip):
            # check when last time updated this file

            if sdfs_master.file_updated_recently(sdfsfilename):
                
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
            print(requester_ip)
            put_info = sdfs_master.handle_put_request(sdfsfilename)
            print(requester_ip)
            # return ack to requester
            return put_info

        server.register_function(put_file_info, 'put_file_info')

        def get_file_info(sdfsfilename):
            # look up who has the file
            # send reqeust to get the file
            return sdfs_master.get_file_replica_list(sdfsfilename)

        server.register_function(get_file_info, 'get_file_info')

        def delete_file_info(sdfsfilename):
            # look up who has file 
            # send request to delete the file
            return False
        server.register_function(delete_file_info, 'delete_file_info')   

# ------------------------- sdfs master functions

# ------------------------- sdfs client functions
        def confirmation_handler():
            try:
                with Timeout(30):
                    command = input('This file was recently updated, are you sure you want to proceed? (yes/no) ')

                    if command == 'yes':
                        return True
                    else:
                        return False

            except Timeout.Timeout:
                return False
        server.register_function(confirmation_handler, 'confirmation_handler')
        

        # def put_done(sdfs_filename, requester_ip):
        #     logging.info('put_done {} {}'.format(sdfs_filename, requester_ip))
        #     global TRANSFER_IN_PROGRESS
        #     if sdfs_filename in TRANSFER_IN_PROGRESS:
        #         TRANSFER_IN_PROGRESS[sdfs_filename] += 1

        #     return True
        # server.register_function(put_done, 'put_done')
        def remote_put_to_replica(target_ip, local_filename, sdfs_filename, ver):
            logging.info('Got Remote Put Request {} {} {}'.format(
                target_ip,
                sdfs_filename,
                ver
            ))
            put_to_replica(target_ip, local_filename, sdfs_filename, ver)
            return True

        server.register_function(remote_put_to_replica, 'remote_put_to_replica')

# ------------------------- sdfs client functions

# ------------------------- sdfs slave functions

        def put_file_data(sdfs_filename, file, ver, requester_ip):
            logging.info('put_file {} {}'.format(sdfs_filename, ver))
            file = file.data
            sdfs.put_file(sdfs_filename, file, ver)

            logging.info('Prep to return to requester {}'.format(requester_ip))

            return True

        server.register_function(put_file_data, 'put_file_data')

        def get_file_data(sdfs_filename, ver):
            logging.info('get_file_data {} {}'.format(
                sdfs_filename, 
                ver,
            ))

            local_ver, file_data = sdfs.get_file(sdfs_filename, ver)

            if local_ver < ver:
                # init repair 
                p = Thread(target=get, args=(sdfs_filename, SDFS_PREFIX + SDFS_PREFIX))
                p.start()


            return {
                'ver': local_ver,
                'file_data': xmlrpc.client.Binary(file_data)
            }

        server.register_function(get_file_data, 'get_file_data')

# ------------------------- sdfs slave functions

        # Run the server's main loop
        server.serve_forever()



# ------------------------- message utils
def get_tcp_client_handle(ip):
    return xmlrpc.client.ServerProxy('http://' + ip + ':8000')

def get_encoded_member_list():
    global member_list
    return get_encoded_msg(member_list)

def get_encoded_msg(msg_list):
    text = str(msg_list)
    return base64.b64encode(text.encode('utf-8'))

def get_decoded_member_list(s):
    text = base64.b64decode(s).decode("utf-8")
    return eval(text)

def encode_binary(msg):
    return base64.b64encode(msg)

def decode_binary(msg):
    return base64.b64decode(msg)
# ------------------------- message utils


# ------------------------- membership list operation
# udp listener thread
def run_udp_server():
    global ALIVE
    global MSG_Q
    serverSocket = socket(AF_INET, SOCK_DGRAM)
    serverSocket.bind(('0.0.0.0', 9000))

    while True:
        # rand = random.randint(0, 10)
        message, address = serverSocket.recvfrom(10240)#65565
        MSG_Q.append(message)
        '''
        remote_member_list = get_decoded_member_list(message)
        if remote_member_list[0][0] == 'join':
            ask_for_join(remote_member_list[0][1])
            continue
        if remote_member_list[0][0] == 'leave':
            handle_leave_request(remote_member_list[0][1])
            continue
        if ALIVE: merge_member_list(remote_member_list)
        '''

def ask_for_join(joiner_ip):
    global member_list
    if joiner_ip in [m[0] for m in member_list]: return True

    logging.debug("Time[{}]: {} is joining.".format(time.time(), joiner_ip))
    new_joiner = (joiner_ip, 0, time.time())
    member_list.append(new_joiner)
    member_list = sorted(member_list, key=itemgetter(0))

    # assumption that there is more than 5 machine at any given time 
    if(len(member_list) == MACHINE_NUM):
        clientSocket = socket(AF_INET, SOCK_DGRAM)
        clientSocket.settimeout(1)
        update_neighbors()
        for member in member_list:
            if member[0] == getfqdn():
                continue
        # send member list to all machines
        message = get_encoded_member_list()
        for member in member_list:
            addr = (gethostbyname(member[0]), 9000)
            clientSocket.sendto(message, addr)
    return True

# check the sorted member_list and find out all 4 neighbors of myself
def update_neighbors():
    global neighbors
    logging.debug("update neighbors is called")
    myIP = getfqdn()
    idx = [m[0] for m in member_list].index(myIP)

    member_list_len = len(member_list)
    neighbors = [
        (idx-2)%member_list_len,
        (idx-1)%member_list_len,
        (idx+1)%member_list_len,
        (idx+2)%member_list_len,
    ]
    logging.debug(neighbors)

# when receive heartbeat messages, merge the received member_list and local one, update information regarding offline machines and/or new join machines
def merge_member_list(remote_member_list):
    global member_list
    logging.debug("merge is called!")
    j = 0
    my_len = len(member_list)
    list_len = len(remote_member_list)
    cur_time = time.time()
    for i in range(list_len):
        # my member_list has machines not appearing in others, skip them
        while (j < my_len and member_list[j][0] < remote_member_list[i][0]):
            j+=1
        # machine appears in both member_list, check heartbeat count to decide whether to update
        if (j < my_len and remote_member_list[i][0] == member_list[j][0]):
            if (remote_member_list[i][1] > member_list[j][1]):
                # update timestampe and heartbeat count only when new count is larger
                member_list[j] = (member_list[j][0], remote_member_list[i][1], cur_time)
            j+=1
        else:
            # my member_list is not displaying a machine that appears in others' list
            tmpList = [m[0] for m in recent_removed]
            domain_name = remote_member_list[i][0]
            if domain_name not in tmpList or recent_removed[tmpList.index(domain_name)][1] < remote_member_list[i][1]:
                # only append it if it is not a machine recently detected to be offline
                member_list.append(remote_member_list[i])
                if domain_name not in tmpList:
                    logging.debug("Time[{}]: {} is joining.".format(time.time(), domain_name))

    member_list = sorted(member_list, key=itemgetter(0))
    update_neighbors()
    logging.debug("---------")
    logging.debug(member_list)
    logging.debug("---------")


def handle_leave_request(leaver_ip):
    global member_list
    logging.info("Time[{}]: {} volunterally left, current member_list: {}".format(time.time(), leaver_ip, member_list))
    leaver_index = [m[0] for m in member_list].index(leaver_ip)
    recent_removed.append(member_list[leaver_index])
    member_list.pop(leaver_index)
    update_neighbors()

def udp_worker():
    global MSG_Q
    global ALIVE

    while True:
        time.sleep(HEARTBEAT_PERIOD)
        while len(MSG_Q) > 0:
            message = MSG_Q.pop()
            remote_member_list = get_decoded_member_list(message)
            if remote_member_list[0][0] == 'join':
                ask_for_join(remote_member_list[0][1])
                continue
            if remote_member_list[0][0] == 'leave':
                handle_leave_request(remote_member_list[0][1])
                continue
            if ALIVE: merge_member_list(remote_member_list)

        # # ask sdfs to check updated memberlist
        global sdfs_master
        sdfs_master.update_member_list(member_list)


# ------------------------- membership list operation


# ------------------------- failure detection
# update the heartbeat count for myself in my member_list
def update_my_heartbeat_count():
    global member_list

    idx = [m[0] for m in member_list].index(getfqdn())
    member_list[idx] = (
        member_list[idx][0], 
        member_list[idx][1]+1, 
        time.time(),
    )
    detect_failure()
    clean_removed_list()

# remove a machine from the recent_removed list after certain amount of time
def clean_removed_list():
    global recent_removed
    cur_time = time.time()
    for member in recent_removed:
        if member[2] < cur_time - REJOIN_COOLDOWN:
           recent_removed.remove(member)

# thread sending heartbeats to all neighbors periodically
def send_heartbeat():
    clientSocket = socket(AF_INET, SOCK_DGRAM)
    clientSocket.settimeout(1)
    global ALIVE
    global member_list

    while True:
        time.sleep(HEARTBEAT_PERIOD)
        # don't send heartbeat if volunterally leave the system or before whole system is initialized
        if not ALIVE or len(member_list) < MACHINE_NUM:
            continue

        update_my_heartbeat_count()
        message = get_encoded_member_list()

        # don't send multiple heartbeat to same machine, only necessary when total machine is less than 5
        has_sent = {}
        for index in neighbors:
            if random.randint(0, 99) < UDP_LOST_RATE or member_list[index][0] in has_sent:
                continue
            addr = (gethostbyname(member_list[index][0]), 9000)
            clientSocket.sendto(message, addr)
            has_sent[member_list[index][0]] = 1

        # # ask sdfs to check updated memberlist
        # global sdfs_master
        # sdfs_master.update_member_list(member_list)


# go through member_list to check if any machine is offline
def detect_failure():
    global member_list
    global recent_removed
    global false_positive_count  # this is used for calculating false positive rate, assuming no machine leave system volunterally
    global sdfs

    cur_time = time.time()
    need_update = False
    for member in member_list:
        if member[2] < cur_time - GRACE_PERIOD:
            need_update = True
            # remove offline machine from member_list but remember its last alive stats
            recent_removed.append(member)
            member_list.remove(member)
            false_positive_count += 1
            logging.debug("Time[{}]: {} has gone offline, current member_list: {}".format(time.time(), member[0], member_list))
            logging.debug("recent_removed: {}".format(recent_removed))
            logging.debug("false_positive_count = {}".format(false_positive_count))
            logging.debug(member_list)
    update_neighbors()

    if need_update:
      fail_recover()

def fail_recover():
    logging.info('Init Failure Repair')
    global member_list
    update_meta = sdfs_master.update_metadata(member_list)

    if len(update_meta) == 0:
        logging.info('But empty update_meta')
        return

    logging.info(update_meta)
    for filename, meta in update_meta.items():
        good_node, ver, new_nodes = meta
        good_node_handle = get_tcp_client_handle(good_node)

        for ip in new_nodes:
            logging.info('Reparing {}@{}'.format(
                filename,
                ip,
            ))
            if good_node == getfqdn():
                put_to_replica(ip, SDFS_PREFIX + filename, filename, ver)
            else:
                good_node_handle.remote_put_to_replica(ip, SDFS_PREFIX + filename, filename, ver)
        
# ------------------------- failure detection


# -------------------------- command line interface

def cli():
    global member_list
    while True:
        try:
            command = input('Enter your command: ')
            if command == 'lsm':
                logging.info("Time[{}]: current number of members = {}".format(time.time(), len(member_list)))
                for member in member_list:
                    logging.info("\t{}".format(member))
            elif command == 'lss':
                logging.info('Time[{}]: {}'.format(time.time(), getfqdn()))
            elif command == 'join':
                init_join()
            elif command == 'leave':
                leave()
            elif command.startswith('put'):
                args = command.split(' ')
                print(args)
                put(args[1], args[2])
            elif command.startswith('get'):
                args = command.split(' ')
                print(args)
                get(args[1], args[2])
            elif command.startswith('ls'):
                args = command.split(' ')
                print(args)
                if len(args) < 2:
                  store()
                else:
                  ls(args[1])
            elif command.startswith('store'):
                store()
            else:
                print("COMMAND NOT SUPPORTED")
        except:
            print("COMMAND NOT SUPPORTED")

def work_done(sdfs_filename, data):
    global TRANSFER_IN_PROGRESS
    global lock 
    lock.acquire()
    if sdfs_filename in TRANSFER_IN_PROGRESS:
        TRANSFER_IN_PROGRESS[sdfs_filename].append(data)
    lock.release()

def init_work(sdfs_filename):
    global TRANSFER_IN_PROGRESS
    global lock 
    lock.acquire()
    TRANSFER_IN_PROGRESS[sdfs_filename] = []
    lock.release()

# action: volunterally leave the system
def leave():
    global ALIVE
    global member_list
    global neighbors
    ALIVE = False
    clientSocket = socket(AF_INET, SOCK_DGRAM)
    clientSocket.settimeout(1)
    reset_member_list = []
    for member in member_list:
        reset_member_list.append((member[0], 0, time.time()))
        if member[0] == getfqdn():
            continue
        addr = (gethostbyname(member[0]), 9000)
        msg_orig = [('leave', getfqdn())]
        msg = get_encoded_msg(msg_orig)
        clientSocket.sendto(msg, addr)
    member_list = reset_member_list

# action: join system, contact introducer
def init_join():
    global ALIVE
    global member_list
    clientSocket = socket(AF_INET, SOCK_DGRAM)
    clientSocket.settimeout(1)
    ALIVE = True
    addr = (gethostbyname(INTRODUCER), 9000)
    msg_orig = [('join', getfqdn())]
    msg = get_encoded_msg(msg_orig) 
    while len(member_list) < MACHINE_NUM or member_list[0][1] < 1:
        clientSocket.sendto(msg, addr)
        time.sleep(1)

def put_to_replica(target_ip, local_filename, sdfs_filename, ver):
    replica_handle = get_tcp_client_handle(target_ip)
    with open(local_filename, 'rb') as f:
        data = f.read()
        # print(type(data))
        replica_handle.put_file_data(sdfs_filename, xmlrpc.client.Binary(data), ver, getfqdn())
    
    logging.info("put {} to {} done".format(
        sdfs_filename,
        target_ip,
    ))
    if not local_filename.startswith(SDFS_PREFIX):
      work_done(sdfs_filename, 1)

def put(local_filename, sdfs_filename):
    print(local_filename, sdfs_filename)
    master_handle = get_tcp_client_handle(INTRODUCER)

    put_info = master_handle.put_file_info(sdfs_filename, getfqdn())
    print(put_info)

    if not put_info:
        print('Put operation aborted.')
        return False

    init_work(sdfs_filename)
    ips = put_info['ips']
    ver = put_info['ver']
    for ip in ips:
        p = Thread(target=put_to_replica, args=(ip, local_filename, sdfs_filename, ver))
        p.start()

    while True:
        time.sleep(HEARTBEAT_PERIOD)
        
        if len(TRANSFER_IN_PROGRESS[sdfs_filename]) >= 2:
            print('quorum count: ', len(TRANSFER_IN_PROGRESS[sdfs_filename]))
            print('Put %s@%d Done.' % (sdfs_filename, ver))

            global lock
            lock.acquire()
            del TRANSFER_IN_PROGRESS[sdfs_filename]
            lock.release()

            return True

def get_from_replica(target_ip, sdfs_filename, ver):
    replica_handle = get_tcp_client_handle(target_ip)

    file_data = replica_handle.get_file_data(sdfs_filename, ver)
    work_done(sdfs_filename, file_data)
    return file_data

def get(sdfs_filename, local_filename):
    logging.info('contacting master for metadata {}'.format(sdfs_filename))

    master_handle = get_tcp_client_handle(INTRODUCER)
    replica_list = master_handle.get_file_info(sdfs_filename)

    ips = replica_list[0]
    ver = replica_list[1]


    if len(replica_list) == 0:
        logging.info('NO FILE NAMED {} FOUND.'.format(sdfs_filename))
        return False

    init_work(sdfs_filename)

    for ip in ips:
        p = Thread(target=get_from_replica, args=(ip, sdfs_filename, ver))
        p.start()


    while True:
        time.sleep(HEARTBEAT_PERIOD)
        if len(TRANSFER_IN_PROGRESS[sdfs_filename]) >= 2:
            print('quorum count: ', len(TRANSFER_IN_PROGRESS[sdfs_filename]))
            print('Get %s@%d Done.' % (sdfs_filename, ver))
            # 
            # return True
            break

    global lock
    lock.acquire()
    for file_meta in TRANSFER_IN_PROGRESS[sdfs_filename]:
        local_ver = file_meta['ver']
        file_data = file_meta['file_data']

        if local_ver == ver:
            with open(local_filename, 'wb') as f:
                f.write(file_data.data)
    del TRANSFER_IN_PROGRESS[sdfs_filename]
    lock.release()

    if local_filename.startswith(SDFS_PREFIX):
        sdfs.update_file_version(sdfs_filename, ver)
        logging.info('repair file {} done'.format(sdfs_filename))
    else:
        logging.info('write to local file {}'.format(local_filename))


def ls(sdfs_filename):
    master_handle = get_tcp_client_handle(INTRODUCER)
    replica_list = master_handle.get_file_info(sdfs_filename)

    ips = replica_list[0]
    ver = replica_list[1]

    print('File: {} Ver:{}'.format(
        sdfs_filename,
        ver
    ))
    for i, ip in enumerate(ips):
        print('Replica {}: {}'.format(
            i,
            ip,
    ))

def store():
    files = sdfs.ls_file()
    for k,v in files.items():
        print('File: {} Ver: {}'.format(
            k,
            v
        ))






# -------------------------- command line interface


if __name__=='__main__':
    logging.basicConfig(filename='mp3.log',level=logging.INFO, filemode='w')

    os.system("rm sdfs/*")
    
    #print to screen as well
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    logging.getLogger('').addHandler(console)

    udp_thread = Thread(target = run_udp_server)
    # tcp_thread = Thread(target = run_tcp_server)
    udp_thread.start()
    # tcp_thread.start()

    udp_worker_thread = Thread(target = udp_worker)
    udp_worker_thread.start()

    hb_thread = Thread(target = send_heartbeat)
    hb_thread.start()

    cli_thread = Thread(target = cli)
    cli_thread.start()
    init_join()
    
    run_tcp_server()
    udp_thread.join()
    # tcp_thread.join()
    hb_thread.join()
    cli_thread.join()
    udp_worker_thread.join()
