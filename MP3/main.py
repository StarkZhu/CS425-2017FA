import os
import logging


from global_vars import *
from tcp import *
from udp import UDPServer
from slave import Slave
from cli import CLI
from threading import Thread

def run_tcp_server(tcp_obj):
    server.register_instance(tcp_obj)
    server.serve_forever()

if __name__ == '__main__':
    logging.basicConfig(filename='mp3.log',level=logging.INFO, filemode='w')
    os.system("rm sdfs/*")
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    logging.getLogger('').addHandler(console)

    sdfs_master = SDFS_Master()
    
    slave = Slave(logging, sdfs_master)
    udpserver = UDPServer(slave)

    udpserver.run_server()

    slave.run()
    cli = CLI(slave, logging)
    cli.run()
    
    slave.init_join()


    tcpserver = TCPServer(slave, sdfs_master, logging)
    run_tcp_server(tcpserver)






