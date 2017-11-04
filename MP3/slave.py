import time
import threading

from socket import *
from global_vars import *


class Slave():
    '''
    def __init__(self):
        self.my_udp_server = None

    def set_udp_server(self, udp_server):
        self.my_udp_server = udp_server
    '''
    def send_heartbeat(self):
        clientSocket = socket(AF_INET, SOCK_DGRAM)
        while True:
            time.sleep(HEARTBEAT_PERIOD)
            addr = (INTRODUCER, UDP_PORT_NUM)
            clientSocket.sendto(b'hello', addr)

    def run(self):
        my_thread = threading.Thread(target=self.send_heartbeat)
        my_thread.start()
