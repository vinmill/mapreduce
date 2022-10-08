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

app = FastAPI()

with open('../configuration.yaml', "r") as f:
    config = yaml.safe_load(f)


# Input file location
# Number of mappers and reducers
# Map and Reduce function : Serialized implementations or file-names (such as map_wc.py).
# IP addresses and port-numbers (either as a config file or explicit list).

class mainserver(object):
    def __init__(self, mappers = config['APP']['MAPPERS'], reducers = config['APP']['REDUCERS'], bind_port = config['APP']['PORT'], data_size=1024, bind_ip = config['APP']['HOST'], fileloc = config['APP']['INPUT'], mapreducefun = config['APP']['FUCNTION']):
        self.connections = 5
        self.table = "raw_data"
        self.data_size = 1024
        self.bind_port = config['APP']['PORT']
        self.bind_ip = config['APP']['HOST']
        self.start_server(bind_ip, bind_port)
    
    def threaded(self, connection):
        try:
            while True:
                dat = connection.recv(self.data_size)

                if not dat:
                    break
                key_value = dat.decode('utf-8')
                key_value = json.loads(key_value)
        finally:
            connection.close()
    
    def start_server(self, bind_ip, bind_port, connections):
        # Set up a TCP/IP server
        tcp_socket = socket(AF_INET, SOCK_STREAM)
        tcp_socket.bind((bind_ip, bind_port))
        tcp_socket.listen(connections)
        
        while True:
            connection, addr = tcp_socket.accept()
            print('SERVER: Connected to: ' + addr[0] + ':' + str(addr[1]))
            self.threaded(connection, addr)