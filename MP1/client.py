import xmlrpc.client
import threading

class myThread (threading.Thread):
	def __init__(self, threadID, s_addr):
		threading.Thread.__init__(self)
		self.threadID = threadID
		self.s_addr = s_addr
	def run(self):
		print ("Starting " + str(self.threadID))
		thread_go(self.threadID, self.s_addr)

def thread_go(threadID, s_addr):
	s = xmlrpc.client.ServerProxy('http://' + s_addr + ':8000')
	try:
		print(s.dgrep('/home/cs425/lbl-pkt-4.sf', '790528000'))
		
	except Exception as e:
		print(e)
		print(str(threadID) + ' is not online, skipping')


server_addrs = []
for i in range(1, 11):
	server_addrs.append('fa17-cs425-g29-' + str(i).zfill(2) + '.cs.illinois.edu');

servers = []
threads = []
for i, s_addr in enumerate(server_addrs):
	threads.append(myThread(i, s_addr))
for thread in threads:
	thread.start()

for thread in threads:
	thread.join()

"""
	servers.append(xmlrpc.client.ServerProxy('http://' + s_addr + ':8000'))

for i,s in enumerate(servers):
	try:
		print(s.dgrep('/home/cs425/lbl-pkt-4.sf', '790528000'))
	except:
		print(str(i) + ' is not online, skipping')
"""
# print(s.dgrep('/Users/dlei/cs425/lbl-pkt-4.sf', '790528000'))

# Print list of available methods
# print(s.system.listMethods())
