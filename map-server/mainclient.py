from socket import *
import yaml
import json
from threading import Thread
import threading
from tcpClient import *
from itertools import zip_longest

print_lock = threading.Lock()

with open('/Users/main/Documents/repos/Cloud-Computing-Assignment2/map-server/configuration.yaml', "r") as f:
    config = yaml.safe_load(f)

separator_token = "<SEP>"
client_sockets = set()
data = None

def getdata(key, table):
    input = json.dumps({"type": "GET", "key": key, "table": table})
    data = client(data=input).data
    return data

def getalldata(table):
    input = json.dumps({"type": "GET", "table": table})
    data = client(data=input).data
    return data

def setdata(key, value, table):
    dat = {"type": "PUT", "key": key, "value": value, "table": table}
    print(dat, type(dat))
    input = json.dumps(dat)
    data = client(data=input)



def textsplitter(mappers, file):
    chunks = []
    with open(file) as inf:
        count = sum(1 for _ in inf)
    numlines = -1 * (- count // mappers)
    with open(file) as inf:
        FILLER = object()
        for group in zip_longest(*([iter(inf)]*numlines), fillvalue=FILLER):
            limit = group.index(FILLER) if group[-1] is FILLER else len(group)
            chunks.append(list(group[:limit]))
    return chunks
    

def sendrecdatmap(connection, file, addr):
    while True:
        # read the bytes from the file
        with open(file, "rb") as f:
            bytes_read = f.read(4096)
        if not bytes_read:
            continue
        connection.sendall(bytes_read)
        bytes_read = connection.recv(4096)
        if bytes_read.decode() == 'mapped':
            connection.send(b'ready')
            bytes_read = connection.recv(4096)
        if not bytes_read:
            print_lock.release()
            break
        setdata(str(addr), bytes_read.decode(), "intermediate_data")
    connection.close()

def start_server(bind_ip, bind_port, connections, file):
    # Set up a TCP/IP server
    tcp_socket = socket(AF_INET, SOCK_STREAM)
    tcp_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    try:
        tcp_socket.bind((bind_ip, bind_port))
        tcp_socket.listen(connections)
    except:
        print('Bind failed. Error.')
    while True:
        client_socket, addr = tcp_socket.accept()
        print('SERVER: Connected to: ' + addr[0] + ':' + str(addr[1]))
        client_sockets.add(client_socket)
        print_lock.acquire()
        t = Thread(target=sendrecdatmap, args=(client_socket,file,addr[1],))
        t.daemon = True
        t.start()

# start_server(config['APP']['HOST'], config['APP']['PORT'], 5 ,config['APP']['INPUT'])
# from pprint import pprint
file = config['APP']['INPUT']    