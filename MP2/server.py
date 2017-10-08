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
MACHINE_NUM = 5
ALIVE = False
HEARTBEAT_PERIOD = 1.0
GRACE_PERIOD = 2.0
REJOIN_COOLDOWN = 12.0

def run_tcp_server():
    # Create server
    with SimpleXMLRPCServer(("0.0.0.0", 8000),
                            requestHandler=RequestHandler) as server:
        server.register_introspection_functions()

        def ping():
            logging.debug("ping")
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

        '''
        def ask_for_join(joiner_ip):
            global member_list
            if joiner_ip in [m[0] for m in member_list]: return True

            logging.info("Time[{}]: {} is joining.".format(time.time(), joiner_ip))
            new_joiner = (joiner_ip, 0, time.time())
            member_list.append(new_joiner)
            member_list = sorted(member_list, key=itemgetter(0))

            # assumption that there is more than 5 machine 
            # at any given time 
            if(len(member_list) == MACHINE_NUM):
                update_neighbors()
                for member in member_list:
                    if member[0] == getfqdn():
                        continue
                    # get member's client, e.g. handle.init()
                    client_handle = get_tcp_client_handle(member[0])
                    client_handle.init(member_list)
            return True
        
        #server.register_function(ask_for_join, 'ask_for_join')

        # initialize all machines in member_list, only called once when enough machines has joined so that the system can start working
        def init(remote_member_list):

            logging.debug('called with init!!')
            global member_list
            member_list = remote_member_list
            logging.debug(member_list)
            update_neighbors()
            logging.debug(neighbors)
            return True

        server.register_function(init, 'init')
        '''
        # Run the server's main loop
        server.serve_forever()

def ask_for_join(joiner_ip):
    global member_list
    if joiner_ip in [m[0] for m in member_list]: return True

    logging.info("Time[{}]: {} is joining.".format(time.time(), joiner_ip))
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

# go through member_list to check if any machine is offline
def detect_failure():
    global member_list
    global recent_removed
    global false_positive_count  # this is used for calculating false positive rate, assuming no machine leave system volunterally
    cur_time = time.time()
    for member in member_list:
        if member[2] < cur_time - GRACE_PERIOD:
            # remove offline machine from member_list but remember its last alive stats
            recent_removed.append(member)
            member_list.remove(member)
            false_positive_count += 1
            logging.info("Time[{}]: {} has gone offline, current member_list: {}".format(time.time(), member[0], member_list))
            logging.debug("recent_removed: {}".format(recent_removed))
            logging.debug("false_positive_count = {}".format(false_positive_count))
            logging.debug(member_list)
    update_neighbors()

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
                    logging.info("Time[{}]: {} is joining.".format(time.time(), domain_name))

    member_list = sorted(member_list, key=itemgetter(0))
    update_neighbors()
    logging.debug("---------")
    logging.debug(member_list)
    logging.debug("---------")

# udp listener thread
def run_udp_server():
    global ALIVE
    serverSocket = socket(AF_INET, SOCK_DGRAM)
    serverSocket.bind(('0.0.0.0', 9000))

    while True:
        rand = random.randint(0, 10)
        message, address = serverSocket.recvfrom(65565)
        remote_member_list = get_decoded_member_list(message)
        if remote_member_list[0][0] == 'join':
            ask_for_join(remote_member_list[0][1])
            continue
        if remote_member_list[0][0] == 'leave':
            handle_leave_request(remote_member_list[0][1])
            continue
        if ALIVE: merge_member_list(remote_member_list)

def handle_leave_request(leaver_ip):
    global member_list
    logging.info("Time[{}]: {} volunterally left, current member_list: {}".format(time.time(), leaver_ip, member_list))
    leaver_index = [m[0] for m in member_list].index(leaver_ip)
    recent_removed.append(member_list[leaver_index])
    member_list.pop(leaver_index)
    update_neighbors()

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
    #ALIVE = True
    #introducer = get_tcp_client_handle(INTRODUCER)
    #introducer.ask_for_join(getfqdn())

# thread monitoring command line
def cli():
    global member_list
    while True:
        command = input('Enter your command: ')
        if command == 'lsm':
            logging.info("Time[{}]: {}".format(time.time(), member_list))
        elif command == 'lss':
            logging.info('Time[{}]: {}'.format(time.time(), getfqdn()))
        elif command == 'join':
            init_join()
        elif command == 'leave':
            leave()
        else:
            print("COMMAND NOT SUPPORTED")

if __name__=='__main__':
    logging.basicConfig(filename='mp2.log',level=logging.INFO, filemode='w')
    #logging.basicConfig(filename='mp2.log',level=logging.DEBUG)
    
    #print to screen as well
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    logging.getLogger('').addHandler(console)

    udp_thread = Thread(target = run_udp_server)
    tcp_thread = Thread(target = run_tcp_server)
    udp_thread.start()
    tcp_thread.start()

    hb_thread = Thread(target = send_heartbeat)
    hb_thread.start()

    cli_thread = Thread(target = cli)
    cli_thread.start()
    init_join()
    
    udp_thread.join()
    tcp_thread.join()
    hb_thread.join()
    cli_thread.join()
