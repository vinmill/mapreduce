from socket import *
import pickle
import os, time
import yaml
 
# Initialize parser
parser = argparse.ArgumentParser()
 
# Adding optional argument
parser.add_argument("-i", "--Input", help = "Input file location")
parser.add_argument("-m", "--Mappers", help = "Number of mapper nodes")
parser.add_argument("-r", "--Reducers", help = "Number of mapper nodes")
parser.add_argument("-f", "--MapReduceFunction", help = "Mapreduce function to use. Either word-count or inverted-index")

 
# Read arguments from command line
args = parser.parse_args()
 
if args.Output:
    print("Displaying Output as: % s" % args.Output)


# Input file location
# Number of mappers and reducers
# Map and Reduce function : Serialized implementations or file-names (such as map_wc.py).
# IP addresses and port-numbers (either as a config file or explicit list).

class mainserver(object):
    def __init__(self, mappers = 5, reducers = 5, bind_port = 27700, data_size=1024, bind_ip = 'localhost', fileloc = 'sherlock.txt', mapreducefun = 'wordcount'):
        object.__init__(self)
        self.filename = 'memcache.pickle'
        self.data_size = data_size
        self.local_dict = {}
        if not os.path.isfile(self.filename):
            with open(self.filename,"wb") as f:
                pickle.dump(self.local_dict, f)
        self.start_server(bind_ip, bind_port, connections)
    
    def 