import sys, time
from socket import *
import yaml

with open('../configuration.yaml', "r") as f:
    config = yaml.safe_load(f)

class client(object):
    def __init__(self, target_port = config['KEYVALSTORE']['PORT'], data_size=1024, target_host = config['KEYVALSTORE']['HOST'], **kwargs):
        object.__init__(self)
        self.__dict__.update(kwargs)
        self.connection(target_host, target_port, data_size, kwargs)

    def now(self):
        return time.ctime(time.time())

    # Create a connection to the server application on port 27700
    def connection(self, target_host, target_port, data_size, kwargs):
        try:
            if len(sys.argv) > 1:
                target_host = sys.argv[1]

            tcp_socket = socket(AF_INET, SOCK_STREAM)
            tcp_socket.connect((target_host, target_port))
            # Receive and print data, as long as the client is sending something
            for key, value in kwargs.items():
                keymod = key + "=" + value
                tcp_socket.send(keymod.encode())
                data = tcp_socket.recv(data_size)
                print('Recieved: {}'.format(data))
        finally:
            tcp_socket.close()

if __name__ == "__main__":
    test_client = client(pdam='lamasfasfdaasdfdsfsdadsfasdffasdf')
