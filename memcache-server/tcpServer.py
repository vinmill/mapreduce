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

class KeyValueStore(object):
    """Simple Key-Value Store that uses sqlite3 database as backend"""

    def __init__(self, connections = 5, bind_port = 27700, data_size=1024, bind_ip = 'localhost', table = 'rawdata'):
        """Opens the database if it exists, otherwise creates it
        """
        self.connections = 5
        self.table = "raw_data"
        self.data_size = 1024
        self.bind_port = config['KEYVALSTORE']['PORT']
        self.bind_ip = config['KEYVALSTORE']['HOST']
        self.filename = config['KEYVALSTORE']['DB']
        self._conn = (
            sqlite3.Connection(self.filename)
            if os.path.exists(self.filename)
            else self.create_database()
        )
        self.start_server(bind_ip, bind_port, connections)

    def create_database(self):
        """Create the key-value database"""
        conn = sqlite3.Connection(self.filename)
        cursor = conn.cursor()
        sql_create_raw = """CREATE TABLE IF NOT EXISTS raw_data (
        key BLOB PRIMARY KEY NOT NULL,
        value blob);"""

        sql_create_intermediate = """CREATE TABLE IF NOT EXISTS intermediate_data (
        key BLOB PRIMARY KEY NOT NULL,
        value blob);"""

        sql_create_output = """CREATE TABLE IF NOT EXISTS output_data (
        key BLOB PRIMARY KEY NOT NULL,
        value blob);"""
        cursor.execute(sql_create_raw)
        cursor.execute(sql_create_intermediate)
        cursor.execute(sql_create_output)
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_key ON raw_data (key);")
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_key ON raw_data (key);")
        cursor.execute("CREATE UNIQUE INDEX IF NOT EXISTS idx_key ON raw_data (key);")
        conn.commit()
        return conn

    def connection(self) -> sqlite3.Connection:
        """Return connection to underlying sqlite3 database"""
        return self._conn
    

    @app.put("/set-value")
    async def put(self,
            key: str = None,
            value: str = None,
            table: Union[str, None] = None):
        update_item_encoded = jsonable_encoder(key, value, table)
        self.set(key, value, table)
        return update_item_encoded
        

    def set(self, key, value, table = 'raw_data'):
        """Set key:value pair"""
        serialized_value = pickle.dumps(value)
        conn = self.connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO " + table + " VALUES (?, ?);", (key, serialized_value,)
        )
        conn.commit()
    
    @app.get("/get-value")
    async def get(self,
            key: str = None,
            table: Union[str, None] = None):
        update_item_encoded = jsonable_encoder(key, table)
        self.get(key, table)
        return update_item_encoded

    def get(self, key, table = 'raw_data'):
        """Get value for key or raise KeyError if key not found"""
        try:
            cursor = self.connection().cursor()
            cursor.execute("SELECT value FROM " + table + " WHERE key = ?;", (key,))
            if result := cursor.fetchone():
                return pickle.loads(result[0])
        except KeyError:
            raise KeyError(key)

    def delete(self, key):
        """Delete key from key-value store"""
        conn = self.connection()
        cursor = conn.cursor()
        cursor.execute("DELETE FROM " + self.table + " WHERE key = ?;", (key,))
        conn.commit()

    def close(self):
        """Close the database"""
        self.connection().close()

    def __del__(self):
        """Try to close the database in case it wasn't already closed. Don't count on this!"""
        with contextlib.suppress(Exception):
            self.close()
    def now(self):
        return time.ctime(time.time())
    
    def threaded(self, connection):
        try:
            while True:
                dat = connection.recv(self.data_size)

                if not dat:
                    break
                key_value = dat.decode('utf-8')
                key_value = json.loads(key_value)
                if 'key' and 'value' and 'table' in key_value:
                    self.set(key_value['key'], key_value['value'], key_value['table'])
                    keystoremsg = 'STORED in' + key_value['table'] + '\r\n'
                    connection.send(keystoremsg.encode())
                elif 'key' and 'value' in key_value:
                    self.set(key_value['key'], key_value['value'])
                    keystoremsg = 'STORED in raw_data \r\n'
                    connection.send(keystoremsg.encode())
                elif 'key' and 'table' in key_value & self.get(key_value['key'], key_value['table']) == None:
                    keystoremsg = 'NOT-STORED in' + key_value['table'] + '\r\n'
                    connection.send(keystoremsg.encode())
                elif 'key' in key_value & self.get(key_value['key']) == None:
                    keystoremsg = 'NOT-STORED in raw_data \r\n'
                    connection.send(keystoremsg.encode())
                else:
                    keystoremsg = 'NOT-STORED\r\n'
                    connection.send(keystoremsg.encode())
                if 'key' in key_value:
                    getvalue = self.get(key_value[0])
                    bytes_obj = bytes(getvalue, "UTF-8")
                print('VALUE {} {} {} bytes\r\n'.format(getvalue, key_value[0], len(bytes_obj)))
                print('{} \r\n'.format(bytes_obj))
        finally:
            connection.close()

        # try:
        #     if len(sys.argv) > 1:
        #         target_host = sys.argv[1]

        #     tcp_socket = socket(AF_INET, SOCK_STREAM)
        #     tcp_socket.connect((target_host, target_port))
        #     # Receive and print data, as long as the client is sending something
        #     for key, value in kwargs.items():
        #         keymod = key + "=" + value
        #         tcp_socket.send(keymod.encode())
        #         data = tcp_socket.recv(data_size)
        #         print('Recieved: {}'.format(data))
        # finally:
        #     tcp_socket.close()
    
        # connection closed
        connection.close()

    def start_server(self, bind_ip, bind_port, connections):
        # Set up a TCP/IP server
        tcp_socket = socket(AF_INET, SOCK_STREAM)
        tcp_socket.bind((bind_ip, bind_port))
        tcp_socket.listen(connections)
        
        while True:
            connection, addr = tcp_socket.accept()
            print('SERVER: Connected to: ' + addr[0] + ':' + str(addr[1]))
            self.threaded(connection)
