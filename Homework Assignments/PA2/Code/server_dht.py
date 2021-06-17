### DEFAULT PYTHON 3.8.3 MODULES
import socket
import threading
import pickle
import argparse
import logging
import time

### Code to Pass Arguments to Server Script through Linux Terminal
parser = argparse.ArgumentParser(description = "This is the Distributed Hash Table Server!")
parser.add_argument('--ip', metavar = 'ip', type = str, nargs = '?', default = socket.gethostbyname(socket.gethostname()))
parser.add_argument('--port', metavar = 'port', type = int, nargs = '?', default = 9000)
args = parser.parse_args()

### SETUP LOGGING
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s:%(message)s')
file_handler = logging.FileHandler('server_dht.log')
file_handler.setFormatter(formatter)
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(formatter)
logger.addHandler(file_handler)
logger.addHandler(stream_handler)

### CONNECTION PROTOCOL
HEADER = 16                 # Size of header
PACKET = 2048               # Size of a packet, multiple packets are sent if message is larger than packet size. 
FORMAT = 'utf-8'            # Message format
ADDR = (args.ip, args.port)  # Address socket server will bind to  

### DEFAULT MESSAGES
DHT_RECORD_MESSAGE = "!DHT_RECORD"
ACTIVATE_MESSAGE = "!ACTIVATE"
UPDATE_MESSAGE = "!UPDATE"
DEACTIVATE_MESSAGE = "!DEACTIVATE"
DISCONNECT_MESSAGE = "!DISCONNECT"

### BIND SOCKET SERVER TO PORT
try:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(ADDR)
except Exception as e:
    raise SystemExit(f"Failed to bind to host: {args.ip} and port: {args.port}, because {e}")

### DISTRIBUTED HASH TABLE (NODE & FILES)
class NodeRecord:
    
    ## CREATE A RECORD
    def __init__(self):
        self.data = {}
    
    ## UPDATE RECORD
    def update(self, addr, file_list):
        self.data[addr] = file_list

    ## DELETE RECORD
    def delete(self, addr):
        self.data.pop(addr)

### MAKE DHT 
DHT = NodeRecord()

### SOCKET NODE-CONNECTION HANDLER
def handle_client(conn, addr):
    
    ## READY CONNECTION
    logger.info(f'{"[NEW CONNECTION]":<26}{addr}')
    conn_time = time.time()

    ## FUNCTION TO SEND MESSAGES ENCODED IN FORMAT TO CLIENT
    def send(msg):
        # message pickled into bytes and HEADER added to message
        msg = pickle.dumps(msg)
        msg = bytes(f'{len(msg):<{HEADER}}', FORMAT) + msg
        if len(msg) > PACKET:    
            for i in range(0, len(msg), PACKET):
                conn.send(msg[i:i+PACKET])
        conn.send(msg)
    
    ## MESSAGE RECEIVER 
    connected = True
    while connected:
        msg = {'main':''}
        
        # RECEIVE MESSAGE HEADER > GET LENGTH OF MESSAGE > SAVE AND DECODE FULL MESSAGE
        msg_length = conn.recv(HEADER)
        if msg_length:
            # Start the process only for a valid header 
            full_msg = b''
            new_msg = True
            # loop to download full message body
            while True:
                # receive message packets
                msg = conn.recv(PACKET)
                
                # get length from header
                if new_msg:
                    msg_len = int(msg_length)
                    full_msg = msg_length
                    new_msg = False
                
                full_msg += msg

                # decode and break out of loop if full message is received
                if len(full_msg)-HEADER == msg_len:
                    msg = pickle.loads(full_msg[HEADER:])
                    break

        # CASE FOR ACTIVATE OR UPDATE DHT DATA
        if msg['main'] == ACTIVATE_MESSAGE or msg['main'] == UPDATE_MESSAGE:
            DHT.update(msg['addr'], msg['file_list'])
            send({'main':DHT_RECORD_MESSAGE,'dht':DHT.data})
            if msg['main'] == ACTIVATE_MESSAGE:
                logger.info(f'{"[NODE ACTIVATED]":<26}{msg["addr"]}')
            else:

                logger.info(f'{"[DHT RECORD SYNCED]":<26}{msg["addr"]}')                

        # CASE FOR DEACTIVATE NODE
        if msg['main'] == DEACTIVATE_MESSAGE:
            DHT.delete(msg['addr'])
            logger.info(f'{"[NODE DEACTIVATED]":<26}{msg["addr"]} Node Deactivated After {time.time()-conn_time}')

        # CASE FOR DISCONNECT NODE
        if msg['main'] == DISCONNECT_MESSAGE:
            connected = False

    ## CLOSE
    logger.info(f'{"[DISCONNECTED]":<26}{addr}')
    conn.close()


### MAIN SERVER THAT IS LISTENING FOR CONNECTIONS ON BINDED PORT,
### ACCEPTS CONNECTIONS AND ASSIGNS A THREAD TO HANDLE CONNECTION.
def start():
    server.listen()
    logger.info(f'{"[LISTENING]":<26}DHT Server is listening on host:{args.ip} and Port:{args.port}')
    while True:

        ## MULTI THREADING CONNECTIONS
        try:
            conn, addr = server.accept()
            thread = threading.Thread(target=handle_client, args=(conn,addr))
            thread.start()
            logger.info(f'{"[ACTIVE CONNECTIONS]":<26}{threading.activeCount() - 1}')

        ## HANDLE ANY OTHER ERRORS
        except Exception as e:
            logger.info(f'Connection error: {e}')


### START SERVER ON BINDED PORT
if __name__ == "__main__":
    logger.info(f'{"[STARTING]":<26}DHT Server is starting...')
    try:
        start()

    ## HANDLE KEYBOARD INTERRUPTS
    except KeyboardInterrupt:
        logger.info(f'{"[KEYBOARD INTERRUPT]":<26}Shutting Down DHT Server')
        logger.info(f'{"[WARNING]":<26}{threading.activeCount() - 1} Threads Active')
        logger.info(f'[SERVER SHUTDOWN]')

    ## HANDLE ANY OTHER ERRORS
    except Exception as e:
        logger.info(f'Failed to Start Server: {e}')
