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


# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

#member_list: (IP, heartbeat_count, timestamp)}
INTRODUCER = 'fa17-cs425-g29-01.cs.illinois.edu'
member_list = []
neighbors = []
recent_removed = []

STATUS_LIVE = 'live'
STATUS_DEAD = 'dead'

MACHINE_NUM = 5


def run_tcp_server():
    # Create server
    with SimpleXMLRPCServer(("0.0.0.0", 8000),
                            requestHandler=RequestHandler) as server:
        server.register_introspection_functions()

        def ping():
            print("ping")
            return "pong"
        server.register_function(ping, 'ping')

        # Register a function under a different name
        def dgrep(path, regEx):
            command = 'cat ' + path + ' | grep ' + regEx
            print(command)
            
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


        def ask_for_join(joiner_ip):
            global member_list

            print("{} ask to join".format(joiner_ip))
            new_joiner = (joiner_ip, 0, time.time())
            member_list.append(new_joiner)
            member_list = sorted(member_list, key=itemgetter(0))

            # assumption that there is more than 5 machine 
            # at any given time 
            if(len(member_list) == MACHINE_NUM):
                #print('reached ')
                #member_list = sorted(member_list, key=itemgetter(0))
                update_neighbors()

                for member in member_list:
                    if member[0] == getfqdn():
                        continue
                    # get member's client, e.g. handle.init()
                    client_handle = get_tcp_client_handle(member[0])
                    print(client_handle, member[0])
                    client_handle.init(member_list)

            return True

        server.register_function(ask_for_join, 'ask_for_join')

        def init(remote_member_list):

            print('called with init!!')
            global member_list
            member_list = remote_member_list
            print(member_list)
            update_neighbors()
            print(neighbors)
            return True

        server.register_function(init, 'init')

        # Run the server's main loop
        server.serve_forever()

def detect_failure():
    global member_list
    global recent_removed
    cur_time = time.time()
    for member in member_list:
        if member[2] < cur_time - 2:
            recent_removed.append(member)
            member_list.remove(member)
    update_neighbors()


def update_neighbors():
    global neighbors
    print("update neighbors is called")
    myIP = getfqdn()
    idx = [m[0] for m in member_list].index(myIP)

    member_list_len = len(member_list)
    neighbors = [
        (idx-2)%member_list_len,
        (idx-1)%member_list_len,
        (idx+1)%member_list_len,
        (idx+2)%member_list_len,
    ]
    print(neighbors)

def get_tcp_client_handle(ip):
    return xmlrpc.client.ServerProxy('http://' + ip + ':8000')

def get_encoded_member_list():
    global member_list
    text = str(member_list)
    return base64.b64encode(text.encode('utf-8'))

def get_decoded_member_list(s):
    text = base64.b64decode(s).decode("utf-8")
    return eval(text)

def merge_member_list(remote_member_list):
    global member_list
    print("merge is called!")
    j = 0
    my_len = len(member_list)
    list_len = len(remote_member_list)
    cur_time = time.time()
    for i in range(list_len):
        while (j < my_len and member_list[j][0] < remote_member_list[i][0]):
            j+=1
        if (j < my_len and remote_member_list[i][0] == member_list[j][0]):
            if (remote_member_list[i][1] > member_list[j][1]):
                member_list[j] = (member_list[j][0], remote_member_list[i][1], cur_time)
            j+=1
        else:
            tmpList = [m[0] for m in recent_removed]
            domain_name = remote_member_list[i][0]
            if domain_name not in tmpList or recent_removed[tmpList.index(domain_name)][1] < remote_member_list[i][1]:
                member_list.append(remote_member_list[i])

    member_list = sorted(member_list, key=itemgetter(0))
    update_neighbors()
    print("---------")
    print(member_list)
    print("---------")


def run_udp_server():
    serverSocket = socket(AF_INET, SOCK_DGRAM)
    serverSocket.bind(('0.0.0.0', 9000))

    while True:
        rand = random.randint(0, 10)
        message, address = serverSocket.recvfrom(65565)
        remote_member_list = get_decoded_member_list(message)
        merge_member_list(remote_member_list)

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

def clean_removed_list():
    global recent_removed
    cur_time = time.time()
    for member in recent_removed:
        if member[2] < cur_time - 12:
           recent_removed.remove(member)

def send_heartbeat():
    clientSocket = socket(AF_INET, SOCK_DGRAM)
    clientSocket.settimeout(1)

    while True:
        time.sleep(1)
        if len(member_list) < MACHINE_NUM:
            continue

        update_my_heartbeat_count()
        message = get_encoded_member_list()

        for index in neighbors:
            addr = (gethostbyname(member_list[index][0]), 9000)
            clientSocket.sendto(message, addr)


if __name__=='__main__':
    # run_udp_server()
    # run_tcp_server()

    import logging
    logging.basicConfig(filename='mp2.log',level=logging.INFO)
    logging.debug('This message should go to the log file')
    logging.info('So should this')
    logging.warning('And this, too')

    udp_thread = Thread(target = run_udp_server)
    tcp_thread = Thread(target = run_tcp_server)
    udp_thread.start()
    tcp_thread.start()

    hb_thread = Thread(target = send_heartbeat)
    hb_thread.start()


    print(getfqdn())
    introducer = get_tcp_client_handle(INTRODUCER)
    introducer.ask_for_join(getfqdn())

    udp_thread.join()
    tcp_thread.join()
    hb_thread.join()

