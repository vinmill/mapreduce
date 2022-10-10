import sys, time
from socket import *
import yaml
import json

with open('/Users/main/Documents/repos/Cloud-Computing-Assignment2/map-server/configuration.yaml', "r") as f:
    config = yaml.safe_load(f)

class client(object):
    def __init__(self, target_port = config['KEYVALSTORE']['PORT'], data_size=1024, target_host = config['KEYVALSTORE']['HOST'], data = {}):
        object.__init__(self)
        self.data = data
        self.connection(target_host, target_port, data_size, data)

    def now(self):
        return time.ctime(time.time())

    # Create a connection to the server application on port 27700
    def connection(self, target_host, target_port, data_size, data):
        try:
            if len(sys.argv) > 1:
                target_host = sys.argv[1]

            tcp_socket = socket(AF_INET, SOCK_STREAM)
            tcp_socket.connect((target_host, target_port))
            # Receive and print data, as long as the client is sending something
            tcp_socket.send(data.encode())
            datar = tcp_socket.recv(data_size)
            self.data = datar
        finally:
            tcp_socket.close()

if __name__ == "__main__":
    todo = {"type": "PUT", "key": "sdam", "value": "something1", "table": "raw_data"}
    todo1 = {"type": "PUT", "key": "pdam", "value": "something2", "table": "raw_data"}
    todo2 = {"type": "PUT", "key": "ddam", "value": "something3", "table": "raw_data"}
    todo3 = {"type": "PUT", "key": "fdam", "value": "something4", "table": "raw_data"}
    todo4 = {"type": "PUT", "key": "gdam", "value": "something5", "table": "raw_data"}
    todo5 = {"type": "GET", "key": "gdam", "value": "something5", "table": "raw_data"}
    todo = json.dumps(todo)
    todo1 = json.dumps(todo1)
    todo2 = json.dumps(todo2)
    todo3 = json.dumps(todo3)
    todo4 = json.dumps(todo4)
    todo5 = json.dumps(todo5)
    test_client = client(data=todo)
    test_client = client(data=todo1)
    test_client = client(data=todo2)
    test_client = client(data=todo3)
    test_client = client(data=todo4)
    test_client = client(data=todo5)
