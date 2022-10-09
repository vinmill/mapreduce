import os
from socket import *
from worker import *
import yaml
import json

with open('../configuration.yaml', "r") as f:
    config = yaml.safe_load(f)

# worker will recieve data from main and input that data to worker function and send the result to main
def recdat(connection, file):
    with open(file + 'tmp.txt', "wb") as f:
        while True:
            # read 1024 bytes from the socket (receive)
            bytes_read = connection.recv(4096)
            if not bytes_read:    
                # nothing is received
                # file transmitting is done
                break
            # write to the file the bytes we just received
            f.write(bytes_read)
    connection.close()

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
    connection.close()
    # close the server socket

def start_server(target_host, target_port, bind_host, bind_port, file):
    # Set up a TCP/IP server
    server_socket = socket(AF_INET, SOCK_STREAM)
    
    try:
        server_socket.bind((bind_host, bind_port))
        server_socket.connect((target_host, target_port))
        server_socket.listen(5)
    except:
        print('Bind failed. Error.')
    recdat(server_socket, file)
    senddat(server_socket, file)


start_server(
    config['APP']['HOST'], 
    config['APP']['PORT'],
    config['MACHINES'][0][0],
    config['MACHINES'][0][1],
    config['WORKERS']['RAWDATA'])