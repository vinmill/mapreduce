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
from worker import *


app = FastAPI()

with open('../configuration.yaml', "r") as f:
    config = yaml.safe_load(f)


# Input file location
# Number of mappers and reducers
# Map and Reduce function : Serialized implementations or file-names (such as map_wc.py).
# IP addresses and port-numbers (either as a config file or explicit list).

class mainserver:
    def __init__(self, mappers = config['APP']['MAPPERS'], 
                reducers = config['APP']['REDUCERS'], 
                data_size = config['APP']['data_size'], 
                bind_port = config['APP']['PORT'], 
                bind_ip = config['APP']['HOST'],
                kvstore_port = config['KEYVALSTORE']['PORT'], 
                kvstore_ip = config['KEYVALSTORE']['HOST'],
                fileloc = config['APP']['INPUT'], 
                mapreducefun = config['APP']['FUCNTION']):
        self.mappers = mappers, 
        self.reducers = reducers
        self.data_size = data_size
        self.bind_port = bind_port
        self.bind_ip = bind_ip
        self.kvstore_port = kvstore_port
        self.kvstore_ip = kvstore_ip
        self.fileloc = fileloc
        self.mapreducefun = mapreducefun
        self.connections = 100
        self.table = "raw_data"
        self.start_server(bind_ip, bind_port)

    def write_to_database(self, file):
        with open(self.fileloc, "r") as f:
            input = f.readlines()
        
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
    
    def start_main_server(self, bind_ip, bind_port):
        # Set up a TCP/IP server
        tcp_socket = socket(AF_INET, SOCK_STREAM)
        tcp_socket.bind((bind_ip, bind_port))
        tcp_socket.listen(self.mappers + self.reducers)
        while True:
            connection, addr = tcp_socket.accept()
            print('SERVER: Connected to: ' + addr[0] + ':' + str(addr[1]))
            self.threaded(connection, addr)

    def splitter(self, number):
        NotImplemented
    
    def sender(self, data):
        try:
            tcp_socket = socket(AF_INET, SOCK_STREAM)
            tcp_socket.connect((self.kvstore_ip, self.kvstore_port))
            # Receive and print data, as long as the client is sending something
            keymod = {'key': self.fileloc[:-3], 'value': }
            tcp_socket.send(keymod.encode())
            data = tcp_socket.recv(self.data_size)
            print('Server Recieved: {}'.format(data))
        finally:
            tcp_socket.close()

    def retrieve(self, table, key):
        NotImplemented

    def retrieveall(self, table):
        NotImplemented

    def partition_data(self, fileloc, mappers):
        with open(fileloc, "r") as f:
            input = f.readlines()
        for x in range(mappers):
            target_host, target_port, data_size,.start()
        for x in range(mappers):
            x.join()

    def executemapper(workers):
        threads = [Thread(target=w.mapper) for w in workers]
        for thread in threads: thread.start()
        for thread in threads: thread.join()
        first, *rest = workers
        return first.outputs

    def executereducer(workers):
        threads = [Thread(target=w.reducer) for w in workers]
        for thread in threads: thread.start()
        for thread in threads: thread.join()
        first, *rest = workers
        return first.outputs

    def mapperfunction(map_worker_class, input_class, config):
        mapworkers = map_worker_class.create_workers(input_class, config)
        return executemapper(mapworkers)

    def reducerfunction(reduce_worker_class, input_class, config):
        reduceworkers = reduce_worker_class.create_workers(input_class, config)
        return executereducer(reduceworkers)

    config = {"data_dir": '/Users/main/Documents/repos/Cloud-Computing-Assignment2/raw-data/',
            "intermediate_dir": '/Users/main/Documents/repos/Cloud-Computing-Assignment2/intermediate_data/'}
    intermediate = mapperfunction(WordCountWorker, TextInputData, config)
    outputs = reducerfunction(WordCountWorker, JsonInputData, config)

    print(outputs)


