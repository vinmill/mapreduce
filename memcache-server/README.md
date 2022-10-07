# memcache-server
## What to Implement

You will be implementing a simpler version of memcached server, along with a client, for testing the server implementation.
### 2.1 TCP-socket server

The server listens on a port, say 9889 for incoming client connections and commands, and stores all the keys and values. In this assignment, the server should store all data in a file system.

You must implement these commands:

    get <key> \r\n
    set <key> <value> \r\n

Keys are text strings (without spaces, newlines, or other special characters). Values are text strings.

A get will fetch the data corresponding to the key and return it to the client. A set should store the value for later retrieval.
#### 2.1.1 Set

Specifically, the set command is whitespace delimited, and consists of two lines:

set <key> <value-size-bytes> \r\n

<value> \r\n

Note that this is a simpler version of the memcached protocol, in which the set command also accepts flags and expiry time, which we will ignore for this assignment.

The server should respond with either "STORED\r\n", or "NOT-STORED\r\n".
#### 2.1.2 Get

Retrieving data is simpler: get <key>\r\n

The server should respond with two lines:

VALUE <key> <bytes> \r\n

<data block>\r\n

After all the items have been transmitted, the server sends the string "END\r\n"
### 2.2 Clients

You should also implement client programs, that connect to the server and make a series of get and set requests. This will be used for testing the server implementation.
### 2.3 Concurrency

The server must be concurrent, and should be able to handle more than one request at a time.
### 2.4 Bonus : Memcached compatibility

As a bonus, you can implement the server such that regular, off-the-shelf memcached clients can connect to your server. For this, you will have to tweak the command parsing etc to support memcached protocol specification such as flags etc.

There are many Memcached clients for testing, for instance https://pymemcache.readthedocs.io/en/latest/getting_started.html
