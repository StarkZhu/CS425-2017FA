import os
import logging


from global_vars import *
from tcp import *
from udp import UDPServer
from slave import Slave
from cli import CLI
from threading import Thread

def run_tcp_server(tcp_obj):
    server.register_instance(TCPserver())
    # Run the server's main loop
    server.serve_forever()


if __name__ == '__main__':
    logging.basicConfig(filename='mp3.log',level=logging.INFO, filemode='w')
    os.system("rm sdfs/*")
    console = logging.StreamHandler()
    console.setLevel(logging.DEBUG)
    logging.getLogger('').addHandler(console)


    slave = Slave(logging)
    udpserver = UDPServer(slave)

    udpserver.run_server()

    slave.run()
    cli = CLI(slave, logging)
    cli.run()
    
    slave.init_join()

    #while True:
    #    input('Hello')





