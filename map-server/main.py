from tcpClient import client
from tcpServer import KeyValueStore
import threading

if __name__ == "__main__":
    server_thread = threading.Thread(target=KeyValueStore)
    server_thread.start()


    
    
        

