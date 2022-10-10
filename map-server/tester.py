from socket import *
import yaml
from typing import List, Union
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from threading import Thread

with open('/Users/main/Documents/repos/Cloud-Computing-Assignment2/map-server/configuration.yaml', "r") as f:
    config = yaml.safe_load(f)

separator_token = "<SEP>"
client_sockets = set()
data = None

def senddat(connection, file):
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
            break
        data = bytes_read
        connection.close()
        # update the progress bar



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
        t = Thread(target=senddat, args=(client_socket,file,))
        t.daemon = True
        t.start()

start_server(config['APP']['HOST'], config['APP']['PORT'], 5, '../test.txt')