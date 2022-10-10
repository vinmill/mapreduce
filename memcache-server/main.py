# from tcpClient import client
from tcpServer import KeyValueStore
import threading

if __name__ == "__main__":
    server_thread = threading.Thread(target=KeyValueStore)
    # client_thread1 = threading.Thread(target=client, kwargs={"one":"oldvalue"})
    # client_thread2 = threading.Thread(target=client, kwargs={"one":"newvalue"})
    # client_thread3 = threading.Thread(target=client, kwargs={"two":"1234old","three":"oldvalue"})
    # client_thread4 = threading.Thread(target=client, kwargs={"two":"1234new","three":"newvalue"})
    # client_thread5 = threading.Thread(target=client)

    # client_threads = [client_thread1, client_thread2, client_thread3, client_thread4, client_thread5]
    # Start all threads
    server_thread.start()

    # i = 0
    # while i < 100:
    #     keystring = "one{}".format(i)
    #     client_thread = threading.Thread(target=client, kwargs={keystring:"newvalue"})
    #     client_thread.start()
    #     client_thread.join()
    #     i += 1

    # for x in client_threads:
    #     x.start()

    # for x in client_threads:
    #     x.join()


    
    
        

