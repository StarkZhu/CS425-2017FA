import os
import logging


from global_vars import *
from tcp import *
from udp import UDPServer
from slave import Slave
from cli import CLI
from threading import Thread

from sava import * 

def run_tcp_server(tcp_obj):
    server.register_instance(tcp_obj)
    server.serve_forever()

if __name__ == '__main__':
    logging.basicConfig(
        filename='mp4.log',
        level=logging.INFO, 
        filemode='w',
        format='%(asctime)s %(message)s', 
        datefmt='%H:%M:%S'
    )
    os.system("rm -f sdfs/*")
    os.system("rm -rf msg_dir; mkdir msg_dir; chmod 777 msg_dir")
    os.system("chmod 777 mp4.log")
    logging.info('system started')
    
    console = logging.StreamHandler()
    formatter = logging.Formatter('%(asctime)s %(message)s', datefmt='%H:%M:%S')
    console.setFormatter(formatter)
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
    sava_worker = SavaWorker(logging)
    tcpserver.set_sava_worker(sava_worker)
    run_tcp_server(tcpserver)






