from socket import *
import yaml
import json
from threading import Thread
from tcpClient import *
from itertools import zip_longest

with open('/Users/main/Documents/repos/Cloud-Computing-Assignment2/map-server/configuration.yaml', "r") as f:
    config = yaml.safe_load(f)

print(config['MACHINES'])