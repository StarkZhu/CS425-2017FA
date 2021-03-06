from xmlrpc.server import SimpleXMLRPCServer
from xmlrpc.server import SimpleXMLRPCRequestHandler
from xml.sax.saxutils import escape
import subprocess
import random
import base64

# Restrict to a particular path.
class RequestHandler(SimpleXMLRPCRequestHandler):
    rpc_paths = ('/RPC2',)

# Create server
with SimpleXMLRPCServer(("0.0.0.0", 8000),
                        requestHandler=RequestHandler) as server:
    server.register_introspection_functions()

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

    # Run the server's main loop
    server.serve_forever()
