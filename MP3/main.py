from global_vars import *

from udp import UDPServer
from slave import Slave

if __name__ == '__main__':
    udpserver = UDPServer()
    slave = Slave()

    udpserver.run_server()
    slave.run()