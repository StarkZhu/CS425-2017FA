import xmlrpc.client
import threading
import unittest

server_addrs = []
for i in range(1, 2):
	server_addrs.append('fa17-cs425-g29-' + str(i).zfill(2) + '.cs.illinois.edu');

class TestMyDistributedSystem(unittest.TestCase):

	def assertEqualWithFailover(self, ret):
		return True

	def test_generate_logs(self):
		# generates logs on every machine
		for i, s_addr in enumerate(server_addrs):
			s = xmlrpc.client.ServerProxy('http://' + s_addr + ':8000')
			print("Generating testing logs for machine {}".format(i))
			s.glog(i)
		

		# do grep and verify
		for i in range(0, 11):
			s = xmlrpc.client.ServerProxy('http://' + s_addr + ':8000')

			# check for unique/rate pattern in one log
			ret = s.dgrep('machine.{}.log'.format(i), '"This is machine-{}"'.format(i))
			self.assertEqual(ret, 'This is machine-{}'.format(i))

			# check for unique/rate pattern in one log 
			ret = s.dgrep('machine.{}.log'.format(i), 'frequent_pattern_')
			# check number matches
			ret = ret.split('\n')
			self.assertEqual(len(ret), 80)
			self.assertTrue(ret[0].startswith('frequent_pattern_'))

			# check for somewhat frequent pattern in one log
			ret = s.dgrep('machine.{}.log'.format(i), 'somewhat_')
			# check number matches
			ret = ret.split('\n')
			self.assertEqual(len(ret), 20)
			self.assertTrue(ret[0].startswith('somewhat_'))

			# check for logs only in even machines
			ret = s.dgrep('machine.{}.log'.format(i), 'EVEN')

			# check number matches
			if i % 2 == 0:
				self.assertEqual(ret, 'EVEN')
			else:
				self.assertEqual(len(ret), 0)

			print('Testing {} DONE'.format(i))

if __name__ == '__main__':
	print('Starting tests')
	unittest.main()
	print('Finished tests')
    