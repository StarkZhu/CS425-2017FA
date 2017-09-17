import xmlrpc.client
import threading
import unittest
from client import multiThreadGrep

server_addrs = []
for i in range(1, 11):
	server_addrs.append('fa17-cs425-g29-' + str(i).zfill(2) + '.cs.illinois.edu');

class TestMyDistributedSystem(unittest.TestCase):

	def assertEqualWithFailover(self, ret):
		return True

	def test_generate_logs(self):
		# SETUP: generates logs on every machine
		for i, s_addr in enumerate(server_addrs):
			# use i+1 to match convention
			s = xmlrpc.client.ServerProxy('http://' + s_addr + ':8000')
			print("Generating testing logs for machine {}".format(i+1))
			s.glog(i+1)
		


		# do grep and verify
		# unique pattern per machine
		for i in range(1, 11):
			print('"This is machine-{}."'.format(i))
			ret = multiThreadGrep('machine.*.log', '"This is machine-{}\."'.format(i))
			
			print(ret)
			ret = '\n'.join([x for x in ret if len(x)!=0])
			self.assertEqual(ret, 'This is machine-{}.\n'.format(i))



		# check for frequent pattern in one log 
		ret = multiThreadGrep('machine.*.log', 'frequent_pattern_')
		# check number matches
		ret = ''.join(ret)
		ret = ret.strip().split('\n')
		self.assertEqual(len(ret), 10*80)
		self.assertTrue(ret[0].startswith('frequent_pattern_'))

		# check for somewhat frequent pattern in one log
		ret = multiThreadGrep('machine.*.log', 'somewhat_')
		# check number matches
		ret = ''.join(ret)
		ret = ret.strip().split('\n')
		self.assertEqual(len(ret), 10*20)
		self.assertTrue(ret[0].startswith('somewhat_'))

		# check for logs only in even machines
		ret = multiThreadGrep('machine.*.log', 'EVEN')
		# check number matches
		ret = ''.join(ret)
		ret = ret.strip().split('\n')
		self.assertEqual(len([x for x in ret if x=='EVEN']), 5)

		print('Testing {} DONE'.format(i))

if __name__ == '__main__':
	print('Starting tests')
	unittest.main()
	print('Finished tests')
    
