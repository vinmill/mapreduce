import os
from socket import *
from worker import *
import yaml
import json

with open('/Users/main/Documents/repos/Cloud-Computing-Assignment2/map-server/configuration.yaml', "r") as f:
    config = yaml.safe_load(f)

# worker will recieve data from main and input that data to worker function and send the result to main
def recdat(connection, file):
    while True:
        # read 1024 bytes from the socket (receive)
        with open(file + 'tmp.txt', "wb") as f:
            bytes_read = connection.recv(4096)
            f.write(bytes_read)
        if not bytes_read:
            continue
        intermediate = mapperfunction(WordCountWorker, TextInputData, config)
        print(intermediate)
        connection.send(b'mapped')
        bytes_read = connection.recv(4096)
        if bytes_read.decode() == 'ready':
            connection.sendall(json.dumps(intermediate, indent=2).encode('utf-8'))
        if connection.recv(4096) == None:
            connection.close()
            break

def senddat(connection, file):
    with open(file + 'tmp.txt', "r") as f:
        intermediate = mapperfunction(WordCountWorker, TextInputData, config)
        while True:
            # read the bytes from the file
            print(intermediate)
            connection.sendall(json.dumps(intermediate, indent=2).encode('utf-8'))
            # if bytes_sent == None:
            #     # file transmitting is done
            break
            # we use sendall to assure transimission in 
            # busy networks
            
    # close the client socket
    # close the server socket

def start_server(target_host, target_port, bind_host, bind_port, file):
    # Set up a TCP/IP server

    server_socket = socket(AF_INET, SOCK_STREAM)
    server_socket.connect((target_host, target_port))
    recdat(server_socket, file)
    while True:
        # server_socket.connect((target_host, target_port))
        senddat(server_socket, file)

start_server(
    config['APP']['HOST'], 
    config['APP']['PORT'],
    config['MACHINES'][0][0],
    config['MACHINES'][0][1],
    config['WORKERS']['RAWDATA'])