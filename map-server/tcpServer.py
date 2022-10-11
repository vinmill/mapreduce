from socket import *
import json
import pickle
import sys
import os, time
import sqlite3
import os.path
import contextlib
import yaml


with open('/Users/main/Documents/repos/Cloud-Computing-Assignment2/map-server/configuration.yaml', "r") as f:
    config = yaml.safe_load(f)

class KeyValueStore(object):
    """Simple Key-Value Store that uses sqlite3 database as backend"""

    def __init__(self, connections = 5, bind_port = 27700, data_size=1024, bind_ip = 'localhost', table = 'raw_data'):
        """Opens the database if it exists, otherwise creates it
        """
        self.connections = 5
        self.table = "raw_data"
        self.data_size = 4096
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
        key varchar PRIMARY KEY NOT NULL,
        value varchar);"""

        sql_create_intermediate = """CREATE TABLE IF NOT EXISTS intermediate_data (
        key varchar PRIMARY KEY NOT NULL,
        value varchar);"""

        sql_create_output = """CREATE TABLE IF NOT EXISTS output_data (
        key varchar PRIMARY KEY NOT NULL,
        value varchar);"""
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


    def set(self, key, value, table = 'raw_data'):
        """Set key:value pair"""
        serialized_value = str(value)
        conn = self.connection()
        cursor = conn.cursor()
        cursor.execute(
            "INSERT OR REPLACE INTO " + table + " VALUES (?, ?);", (key, serialized_value,)
        )
        conn.commit()
    

    def getall(self, table = 'raw_data'):
        """Get value for key or raise KeyError if key not found"""
        try:
            cursor = self.connection().cursor()
            cursor.execute("SELECT value FROM " + table)
            if result := cursor.fetchall():
                return bytes(str(result), 'utf-8')
            else:
                return b'N/A'
        except KeyError:
            raise KeyError(table)

    def get(self, key, table = 'raw_data'):
        """Get value for key or raise KeyError if key not found"""
        try:
            cursor = self.connection().cursor()
            cursor.execute("SELECT value FROM " + table + " WHERE key = ?;", (key,))
            if result := cursor.fetchone():
                return bytes(str(result[0]), 'utf-8')
            else:
                return b'N/A'
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
            dat = connection.recv(self.data_size)

            key_value = json.loads(dat)

            if key_value['type'] == 'GET':
                getvalue = self.get(key_value['key'], key_value['table'])
                connection.sendall(getvalue)
            elif key_value['type'] == 'PUT':
                getvalue = self.set(key_value['key'], key_value['value'], key_value['table'])
                connection.sendall(b'STORED')
            elif key_value['type'] == 'GETALL':
                getvalue = self.getall(key_value['table'])
                connection.sendall(getvalue)           
            # print(userdata)
        finally:
            connection.close()
    
        # connection closed
        connection.close()

    def start_server(self, bind_ip, bind_port, connections):
        # Set up a TCP/IP server
        tcp_socket = socket(AF_INET, SOCK_STREAM)
        tcp_socket.setsockopt(SOL_SOCKET, SO_REUSEADDR, 1)
        tcp_socket.bind((bind_ip, bind_port))
        tcp_socket.listen(connections)
        
        while True:
            connection, addr = tcp_socket.accept()
            print('SERVER: Connected to: ' + addr[0] + ':' + str(addr[1]))
            self.threaded(connection)
