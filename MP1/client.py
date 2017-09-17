import xmlrpc.client
import threading
import sys 
import time
import base64
import traceback

# for debug use
FLAG_DEBUG = False

# thread class, used to have 10 threads doing RPC in parallel
class myThread (threading.Thread):
	def __init__(self, threadID, s_addr, file_path, regEx, result):
		threading.Thread.__init__(self)
		self.threadID = threadID
		self.s_addr = s_addr
		self.file_path = file_path
		self.regEx = regEx
		self.result = result;
	def run(self):
		thread_go(self.threadID, self.s_addr, self.file_path, self.regEx, self.result)

def thread_go(threadID, s_addr, file_path, regEx, result):
	s = xmlrpc.client.ServerProxy('http://' + s_addr + ':8000')
	
	try:
		ret = s.dgrep(file_path, regEx)
		ret_decoded = base64.b64decode(ret.data).decode("utf-8")
		result[threadID - 1] = ret_decoded
	# if some servers are down, print error message and don't wait
	except Exception as e:
		result[threadID - 1] = 'Error: ' + str(threadID) + ' is not online, skipping'
		if FLAG_DEBUG:
			print("threadID {}:".format(threadID))
			print(e)
			traceback.print_tb(e.__traceback__)
	
# create 10 threads to do jobs and wait till receiving all outputs before return
def multiThreadGrep(file_path, regEx):
	result = [None] * 10
	server_addrs = []
	for i in range(1, 11):
		server_addrs.append('fa17-cs425-g29-' + str(i).zfill(2) + '.cs.illinois.edu');

	servers = []
	threads = []
	# create 10 threads
	for i, s_addr in enumerate(server_addrs):
		threads.append(myThread(i+1, s_addr, file_path, regEx, result))
	
	for thread in threads:
		thread.start()

	for thread in threads:
		thread.join()

	return result


if __name__ == '__main__':
	# measure the start of the time
	start = time.time()

	if len(sys.argv) > 1:
		print("Run with parameters path:{} regEx:{}".format(
			sys.argv[1], 
			sys.argv[2]),
		)
		result = multiThreadGrep(sys.argv[1], sys.argv[2])
	else:
		print("Run with default parameter")
		result = multiThreadGrep('/home/cs425/lbl-pkt-4.sf', '790528000')
	
	# end of the time
	end = time.time()

	total_count = 0
	line_counts = []
	for i, s in enumerate(result):
		f = open('./outputs/' + str(i+1) +'.output', 'w+')
		# counting and adding number of results found for each machine
		individual_result = result[i].splitlines()
		line_counts.append(len(individual_result));
		if (len(individual_result) == 1 and ('No such file or directory' in individual_result[0] or 'is not online, skipping' in individual_result[0])):
			total_count -= 1
			line_counts[-1] -= 1
		total_count += len(individual_result)
		# write to file instead of printing to terminal
		f.write("Output from machine {} :\n".format(i+1))
		f.write(result[i])
		f.write("\n")
	# output counts for each machine and total
	for i, val in enumerate(line_counts):
		print("Machine {} outputs {} lines.".format(i+1, val));
	print("Total found: {}".format(total_count))
	print("Time elapsed to retrieve result: %fs" % (end - start))

