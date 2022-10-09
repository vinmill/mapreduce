from socket import *
import json
import pickle
import os, time
import sqlite3
import os.path
import contextlib
import yaml
from typing import List, Union
from fastapi import FastAPI
from fastapi.encoders import jsonable_encoder
from threading import Thread

with open('../configuration.yaml', "r") as f:
    config = yaml.safe_load(f)

separator_token = "<SEP>"
client_sockets = set()

def senddat(connection, file):
    with open(file, "rb") as f:
        while True:
            # read the bytes from the file
            bytes_read = f.read(4096)
            if not bytes_read:
                # file transmitting is done
                break
            # we use sendall to assure transimission in 
            # busy networks
            connection.sendall(bytes_read)
            # update the progress bar
    connection.close()

def recdat(connection, file):
    while True:
        # read the bytes from the file
        bytes_read = connection.recv(4096)
        print(bytes_read.decode())
        if not bytes_read:
            # file transmitting is done
            break
    connection.close()

def listen_for_client(cs):
    """
    This function keep listening for a message from `cs` socket
    Whenever a message is received, broadcast it to all other connected clients
    """
    while True:
        msg = ''
        try:
            # keep listening for a message from `cs` socket
            msg = cs.recv(1024).decode()
        except Exception as e:
            # client no longer connected
            # remove it from the set
            print(f"[!] Error: {e}")
            client_sockets.remove(cs)
        else:
            msg = msg.replace(separator_token, ": ")


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
        senddat(client_socket, file)
        recdat(client_socket, file)
        client_sockets.add(client_socket)
        t = Thread(target=listen_for_client, args=(client_socket,))
        t.daemon = True
        t.start()

start_server(config['APP']['HOST'], config['APP']['PORT'], 5, '../test.txt')