import sys
from socket import *
import yaml
import json
from threading import Thread
from tcpClient import *
from itertools import zip_longest
from workerserver import *

with open('/Users/main/Documents/repos/Cloud-Computing-Assignment2/map-server/configuration.yaml', "r") as f:
    config = yaml.safe_load(f)

separator_token = "<SEP>"
client_sockets = set()
dat = None

def sendmsgm(connection, msg):
    # Prefix each message with a 4-byte length (network byte order)
    msg = struct.pack('>I', len(msg)) + msg
    connection.sendall(msg)

def recvmsgm(connection):
    # Read message length and unpack it into an integer
    raw_msglen = recvallm(connection, 4)
    if not raw_msglen:
        return None
    msglen = struct.unpack('>I', raw_msglen)[0]
    # Read the message data
    return recvallm(connection, msglen)

def recvallm(connection, filesize):
    # Helper function to recv n bytes or return None if EOF is hit
    data = bytearray()
    while len(data) < filesize:
        packet = connection.recv(filesize - len(data))
        if not packet:
            return None
        data.extend(packet)
    return data

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
    input = json.dumps(dat)
    client(data=input)

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

def sendrecdatmap(connection, table, addr):
    while True:
        # read the bytes from the file
        bytes_read = getdata(addr, table)
        sendmsgm(connection, bytes_read)
        msg = recvmsgm(connection)
        if msg:
            if msg.decode() == 'mapped':
                sendmsgm(connection,b'ready')
                bytes_read = recvmsgm(connection)
                setdata(str(addr), bytes_read.decode(), "intermediate_data")
            elif msg.decode() == 'Done':
                print(msg.decode())
                break
        elif not msg:
            break
    connection.close()

def start_server_main(bind_ip, bind_port, connections, file):
    # Set up a TCP/IP server
    chunk = 0
    chunks = textsplitter(connections, file)
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
        setdata(str(addr[1]), chunks[chunk], "raw_data")
        print(client_sockets)
        t = Thread(target=sendrecdatmap, args=(client_socket,"raw_data",addr[1],))
        chunk += 1
        t.daemon = True
        t.start()
        

machines = len(config['MAPPERS'])
serverthread = Thread(target=
    start_server_main, args=(config['APP']['HOST'], 
    config['APP']['PORT'], 
    machines,
    config['APP']['INPUT'],))
serverthread.start()
for i in range(len(config['MAPPERS'])):
    mapperthread = Thread(target=start_server_worker, args=(
    config['APP']['HOST'], 
    config['APP']['PORT'],
    config['MAPPERS'][i][0],
    config['MAPPERS'][i][1],
    config,))
    mapperthread.start()
    mapperthread.join()
# from pprint import pprint
