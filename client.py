import xmlrpc.client

s = xmlrpc.client.ServerProxy('http://localhost:8000')

print(s.dgrep('/Users/dlei/cs425/lbl-pkt-4.sf', '790528000'))

# Print list of available methods
print(s.system.listMethods())
