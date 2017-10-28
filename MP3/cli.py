
class CLI():
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