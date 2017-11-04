import base64
import xmlrpc.client



def encode_obj(obj):
    text = str(obj)
    return base64.b64encode(text.encode('utf-8'))

def decode_obj(msg):
    text = base64.b64decode(msg).decode("utf-8")
    return eval(text)

def rpc(ip, func, *args):
    handle = get_tcp_client_handle(ip)
    return getattr(handle, func, *args)

def get_tcp_client_handle(ip):
    return xmlrpc.client.ServerProxy('http://' + ip + ':8000')