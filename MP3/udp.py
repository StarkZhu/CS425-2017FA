from socket import *
from global_vars import *
from threading import Thread

import time
import logging


class UDPServer():
    def __init__(self):
        self._udp_socket = socket(AF_INET, SOCK_DGRAM)
        self._udp_socket.bind(('0.0.0.0', 9000))
        self._worker_queue = []
        self._udp_count = 0

    # def send_msg(self):

    def server_thread(self):
        while True:
            message, address = self._udp_socket.recvfrom(10240)#65565
            self._worker_queue.append(message)
            self._udp_count += 1

    def worker_thread(self):
        while True:
            time.sleep(HEARTBEAT_PERIOD)
            print(self._udp_count)
            while len(self._worker_queue) > 0:
                message = self._worker_queue.pop()

            '''
            
                message = MSG_Q.pop()
                remote_member_list = get_decoded_member_list(message)
                if remote_member_list[0][0] == 'join':
                    ask_for_join(remote_member_list[0][1])
                    continue
                if remote_member_list[0][0] == 'leave':
                    handle_leave_request(remote_member_list[0][1])
                    continue
                if ALIVE: merge_member_list(remote_member_list)
            '''

    def run_server(self):
        udp_thread = Thread(target = self.server_thread)
        worker_thread = Thread(target = self.worker_thread)
        udp_thread.start()
        worker_thread.start()

