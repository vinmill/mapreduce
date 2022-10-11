import os
from socket import *
from workers import *
import yaml
import json
import struct

# worker will recieve data from main and input that data to worker function and send the result to main
def sendmsgw(connection, msg):
    # Prefix each message with a 4-byte length (network byte order)
    msg = struct.pack('>I', len(msg)) + msg
    connection.sendall(msg)

def recvmsgw(connection):
    # Read message length and unpack it into an integer
    raw_msglen = recvallw(connection, 4)
    if not raw_msglen:
        return None
    msglen = struct.unpack('>I', raw_msglen)[0]
    # Read the message data
    return recvallw(connection, msglen)

def recvallw(connection, filesize):
    # Helper function to recv n bytes or return None if EOF is hit
    data = bytearray()
    while len(data) < filesize:
        packet = connection.recv(filesize - len(data))
        if not packet:
            return None
        data.extend(packet)
    return data

def recsenddat(connection, config):
    while True:
        # read 1024 bytes from the socket (receive)
        bytes_read = recvmsgw(connection)
        print(bytes_read.decode())
        intermediate = mapperfunction(WordCountWorker, bytes_read.decode())
        print(intermediate)
        sendmsgw(connection, b'mapped')
        msg = recvmsgw(connection)
        if msg:
            if msg.decode() == 'ready':
                sendmsgw(connection, json.dumps(intermediate).encode('utf-8'))
        elif not msg:
            sendmsgw(connection,b'Done')
            break
    connection.close()


def start_server_worker(target_host, target_port, bind_host, bind_port, file):
    # Set up a TCP/IP server

    server_socket = socket(AF_INET, SOCK_STREAM)
    server_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
    server_socket.bind((bind_host, bind_port))
    server_socket.connect((target_host, target_port))
    while True:
        # server_socket.connect((target_host, target_port))
        recsenddat(server_socket, config)