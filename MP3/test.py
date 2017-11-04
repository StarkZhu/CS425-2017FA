
from multiprocessing import Process, Value, Array
from threading import Thread

from socket import *
import time

TCP_COUNT = 0
UDP_COUNT = 0
OFFSET = 5

def tcp_server():
    global TCP_COUNT

    serverSocket = socket(AF_INET, SOCK_STREAM)
    serverSocket.bind(('0.0.0.0', 7000 + OFFSET))
    serverSocket.listen(1)

    
    while True:
        # rand = random.randint(0, 10)
        conn, address = serverSocket.accept()#65565
        data = conn.recv(1024)
        if not data: continue
        # print(data)
        # conn.send('pong')
        TCP_COUNT += 1


def udp_server():
    global UDP_COUNT
    serverSocket = socket(AF_INET, SOCK_DGRAM)
    serverSocket.bind(('0.0.0.0', 9000 + OFFSET))

    while True:
        # rand = random.randint(0, 10)
        message, address = serverSocket.recvfrom(10240)#65565
        # print(time.time(), address)
        UDP_COUNT += 1
        time.sleep(0.01)

if __name__=='__main__':



    udp_server_thread = Thread(target = udp_server)
    udp_server_thread.start()

    tcp_server_thread = Thread(target = tcp_server)
    tcp_server_thread.start()

    while True:
        time.sleep(1)

        udp_socket = socket(AF_INET, SOCK_DGRAM)
        udp_socket.sendto(b'udp_message', ('fa17-cs425-g29-01.cs.illinois.edu', 9000 + OFFSET))

        tcp_socket = socket(AF_INET, SOCK_STREAM)
        tcp_socket.connect(('fa17-cs425-g29-01.cs.illinois.edu', 7000 + OFFSET))
        tcp_socket.send(b'tcp_message')
        # data = tcp_socket.recv(64)

        print(UDP_COUNT, TCP_COUNT)


