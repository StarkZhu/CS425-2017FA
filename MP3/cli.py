import time
from socket import *
from threading import Thread

class CLI():
    def __init__(self, slave, logger):
        self._slave = slave
        self._logger = logger

    def run(self):
        self._logger.info("CLI Started")
        cli_thread = Thread(target = self.run_cli)
        cli_thread.start()

    def run_cli(self):
        while True:
            command = input('Enter your command: ')
            try:
                # time.sleep(10)
                if command == 'lsm':
                    self._logger.info("Time[{}]: current number of members = {}".format(
                        time.time(), 
                        len(self._slave._member_list)
                    ))
                    for member in self._slave._member_list:
                        self._logger.info("\t{}".format(member))
                elif command == 'lss':
                    self._logger.info('Time[{}]: {}'.format(
                        time.time(), getfqdn())
                    )
                elif command == 'join':
                    self._slave.init_join()
                elif command == 'leave':
                    self._slave.leave()
                
                elif command.startswith('put'):
                    args = command.split(' ')
                    print(args)
                    self._slave.put(args[1], args[2])
                elif command.startswith('get'):
                    args = command.split(' ')
                    print(args)
                    self._slave.get(args[1], args[2])
                elif command.startswith('ls'):
                    args = command.split(' ')
                    print(args)
                    if len(args) < 2:
                      self._slave.store()
                    else:
                      self._slave.ls(args[1])
                elif command.startswith('store'):
                    self._slave.store()
            except:
                print("COMMAND NOT SUPPORTED")

