### DEFAULT PYTHON 3.8.3 MODULES
import socket
import threading
import pickle
import argparse
import logging
import time
import os
import hashlib

### Code to Pass Arguments to Server Script through Linux Terminal
parser = argparse.ArgumentParser(description = "This is the Node in the DHT Architecture!")
parser.add_argument('--ip', metavar = 'ip', type = str, nargs = '?', default = socket.gethostbyname(socket.gethostname()))
parser.add_argument('--port', metavar = 'port', type = int, nargs = '?', default = 9001)
parser.add_argument('--name', metavar = 'name', type = str, nargs = '?', default = 'Node')
parser.add_argument('--dir', metavar = 'dir', type = str, nargs = '?', default = './hosted_files')
parser.add_argument('--dht_ip', metavar = 'dht_ip', type = str, nargs = '?', default = socket.gethostbyname(socket.gethostname()))
parser.add_argument('--dht_port', metavar = 'dht_port', type = int, nargs = '?', default = 9000)
args = parser.parse_args()

### MAKE DIRECTORY TO LOG OUTPUT
if not os.path.exists('./logs'):
    os.makedirs('./logs')
    print(f'{"[SETUP]":<26}./logs directory created.')

### MAKE DIRECTORY TO HOST FILES FROM IF NOT MADE
if not os.path.exists(args.dir):
    os.makedirs(args.dir)
    print(f'{"[SETUP]":<26}{args.dir} directory created. Keep files which you want to host here.')

### SETUP LOGGING
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s:%(message)s')
file_handler = logging.FileHandler(f'./logs/{args.name}.log')
file_handler.setFormatter(formatter)
logger.addHandler(file_handler)

### CONNECTION PROTOCOL & ADDRESSES
HEADER = 16                 # Size of header
PACKET = 2048               # Size of a packet, multiple packets are sent if message is larger than packet size. 
FORMAT = 'utf-8'            # Message format
ADDR = (args.ip, args.port)  # Address socket server will bind to  
DHT_ADDR = (args.dht_ip, args.dht_port)

### DEFAULT MESSAGES
REQ_FILE_LIST_MESSAGE = "!FILE_LIST"
RES_FILE_LIST_MESSAGE = "!RES_FILE_LIST"
DOWNLOAD_MESSAGE = "!DOWNLOAD"
RES_DOWNLOAD_MESSAGE = "!RES_DOWNLOAD"
DHT_RECORD_MESSAGE = "!DHT_RECORD"
ACTIVATE_MESSAGE = "!ACTIVATE"
UPDATE_MESSAGE = "!UPDATE"
DEACTIVATE_MESSAGE = "!DEACTIVATE"
DISCONNECT_MESSAGE = "!DISCONNECT"

### BIND NODE TO PORT
try:
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(ADDR)
except Exception as e:
    raise SystemExit(f"Failed to bind to host: {args.ip} and port: {args.port}, because {e}")

global_record = {}

### A MULTITHREADED NODE CLASS TO CREATE HANDLERS
class NodeThread(threading.Thread):

    ## CONSTRUCTOR (THREAD A NEW OR EXISTING CONNECTION)
    def __init__(self, conn=None, addr=DHT_ADDR):
        threading.Thread.__init__(self)
        self.addr = addr
        if conn is None:
            self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.conn.connect(addr)
        else:
            self.conn = conn
        
        # HANDLER PARAMETERS
        self.listen = True
        self.buffer_file_list = None
        self.buffer_file_data = None
        self.buffer_down_size = None
        logger.info(f'{"[NEW CONNECTION]":<26}{self.addr}')

    ##
    ### BASIC FUNCTIONS
    ##

    ## SEND MESSAGE FUNCTION
    def send(self,msg):
        # message pickled into bytes and HEADER added to message
        msg = pickle.dumps(msg)
        msg = bytes(f'{len(msg):<{HEADER}}', FORMAT) + msg
        if len(msg) > PACKET:
            for i in range(0, len(msg), PACKET):
                self.conn.send(msg[i:i+PACKET])
            return len(msg)
        self.conn.send(msg)
        return len(msg)

    ## FUNCTION TO GET FILE LIST FROM LOCAL HOSTED DIRECTORY
    def localFileList(self):
        return [f for f in os.listdir(args.dir) if os.path.isfile(os.path.join(args.dir, f))]

    ## FUNCTION TO SAFELY DISCONNECT AND CLOSE CONNECTION
    def disconnect(self):
        self.listen = False
        msg = {'main':DISCONNECT_MESSAGE}
        self.send(msg)
        self.conn.close()
        logger.info(f'{"[DISCONNECTED]":<26}{self.addr}')

    ##
    ### DHT SERVER (NODE HANDLER) FUNCTIONS
    ##
    
    ## FUNCTION TO ACTIVATE NODE 
    def activate(self):
        msg = {'main':ACTIVATE_MESSAGE, 'addr':ADDR, 'file_list':self.localFileList()}
        self.send(msg)
        logger.info(f'{"[NODE ACTIVE]":<26}')

    ## FUNCTION TO SYNCHRONIZE
    def sync(self):
        msg = {'main':UPDATE_MESSAGE, 'addr':ADDR, 'file_list':self.localFileList()}
        self.send(msg)
        print('\nSynchronized With DHT Server')
        logger.info(f'{"[NODE SYNCED]":<26}')

    ## FUNCTION TO DEACTIVATE
    def deactivate(self):
        msg = {'main':DEACTIVATE_MESSAGE, 'addr':ADDR}
        self.send(msg)
        logger.info(f'{"[NODE DEACTIVE]":<26}')

    ##
    ### INTER NODE FUNCTIONS
    ##
    
    ## FUNCTION TO GET FILE LIST FROM REMOTE NODE
    def getFileList(self):
        self.send({'main':REQ_FILE_LIST_MESSAGE})
        # USE RECEIVER & BUFFER TO RECEIVE
        while not self.buffer_file_list:
            time.sleep(.1)
        return self.buffer_file_list

    ## FUNCTION TO DOWNLOAD FILES FROM REMOTE NODE
    def download(self, download_list):
        fail_list = []
        # ITERATE DOWNLOAD LIST
        for d in download_list:
            # REQUEST DOWNLOAD
            down_file_time = time.time()
            self.send({'main':DOWNLOAD_MESSAGE,'file_name':d})
            # RESPONSE RECEIVE
            while not self.buffer_file_data:
                time.sleep(.1)
            # PROCEED IF RIGHT RESPONSE
            if self.buffer_file_data['file_name'] == d:
                # GENERATE LOCAL MD5
                md5_mirror = hashlib.md5(self.buffer_file_data['file_data']).hexdigest()
                # SAVE IF INTEGRITY CHECK SUCCESSFUL AND REPORT STATS
                if self.buffer_file_data['md5'] == md5_mirror:
                    file_mirror = open(os.path.join(args.dir,d), 'wb')
                    file_mirror.write(self.buffer_file_data['file_data'])
                    file_mirror.close()
                    down_file_time = time.time()-down_file_time
                    logger.info(f'{"[DOWNLOAD INFO]":<26}{self.buffer_file_data["file_name"]} downloaded from {self.addr}')
                    logger.info(f'{"[DOWNLOAD STAT]":<26}{self.buffer_down_size} Bytes <- {self.addr} in {down_file_time} Seconds')
                    print(f'\n{d}\nmd5: {md5_mirror}\nIntegrity check pass, downloaded successfully!')
                    print(f'Downloaded in {down_file_time} seconds')
                    self.buffer_file_data = None
                    self.buffer_down_size = None
                # DON'T SAVE IF INTEGRITY CHECK FAILS, TRY AGAIN LATER
                else:
                    print(f'\n{d}\nFile integrity failures.')
                    self.buffer_file_data = None
                    self.buffer_down_size = None
                    fail_list.append(d)
            # IF WRONG FILE, TERMINATE
            else:
                self.buffer_file_data = None
                self.buffer_down_size = None
                fail_list.append(d)
        # RETURN LIST OF FAILED FILE DOWNLOADS
        return fail_list

    ## RECEIVER (CLIENT HANDLER FOR NODE)
    def run(self):
        while self.listen:
            msg = {'main':''}
            
            # RECEIVE MESSAGE HEADER > GET LENGTH OF MESSAGE > SAVE AND DECODE FULL MESSAGE
            msg_length = self.conn.recv(HEADER)
            if msg_length:
                # Start the process only for a valid header 
                full_msg = b''
                new_msg = True
                # loop to download full message body
                while True:
                    # receive message packets
                    msg = self.conn.recv(PACKET)
                    
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
                
                # RECEIVE & UPDATE LOCAL DHT RECORD
                if msg['main'] == DHT_RECORD_MESSAGE:
                    global global_record
                    global_record = msg['dht']

                # CASE: REQ FOR FILE LIST, SEND LOCAL FILE LIST 
                if msg['main'] == REQ_FILE_LIST_MESSAGE:
                    res = {'main':RES_FILE_LIST_MESSAGE, 'file_list':self.localFileList()}
                    self.send(res)
                
                # CASE: RES FOR A FILE LIST REQUEST, SAVE IN BUFFER
                if msg['main'] == RES_FILE_LIST_MESSAGE:
                    self.buffer_file_list = msg['file_list']
                
                # CASE: DOWNLOAD REQUEST
                if msg['main'] == DOWNLOAD_MESSAGE:
                    up_time = time.time()
                    # FIND FILE
                    file_name = os.path.join(args.dir, msg['file_name'])
                    file_open = open(file_name,'rb')
                    file_data = file_open.read()
                    # GENERATE MD5
                    md5 = hashlib.md5(file_data).hexdigest()
                    # SEND MD5 AND FILE BINARY DATA
                    res = {'main':RES_DOWNLOAD_MESSAGE, 'file_name':msg['file_name'], 'md5':md5, 'file_data':file_data}
                    up_size = self.send(res)
                    # REPORT THE UPLOAD STATS
                    up_time = time.time()-up_time
                    logger.info(f'{"[UPLOAD INFO]":<26}{msg["file_name"]} sent to {self.addr}')
                    logger.info(f'{"[UPLOAD STAT]":<26}{up_size} Bytes -> {self.addr} in {up_time} Seconds')

                # CASE: RES FOR DOWNLOAD REQUEST, SAVE TO BUFFER
                if msg['main'] == RES_DOWNLOAD_MESSAGE:
                    self.buffer_down_size = len(full_msg)
                    self.buffer_file_data = msg

                # CASE: DISCONNECTING REMOTE NODE, RELEASE CONNECTION
                if msg['main'] == DISCONNECT_MESSAGE:
                    logger.info(f'{"[DISCONNECTED]":<26}{self.addr}')
                    self.listen = False
                    self.conn.close()

### KEEP LISTING FOR CONNECTIONS ON BINDED PORT
def portListener():
    server.listen()
    while True:
        ## MULTI THREADING CONNECTIONS
        try:
            conn, addr = server.accept()
            new_node = NodeThread(conn, addr)
            new_node.start()
            logger.info(f'{"[ACTIVE CONNECTIONS]":<26}{threading.activeCount() - 1}')

        ## HANDLE ANY OTHER ERRORS
        except Exception as e:
            break

        except KeyboardInterrupt:
            break

### SELECT A NODE TO DOWNLOAD FILES FROM, FROM THE DHT RECORD
def selectNodeFromDHT():
    node_list = list(global_record)
    val_list = list(global_record.values())
    ## DISPLAY TABLE
    print(f'\nINDEX {"Node Address":^22}    {"Files":^22}')
    for node in node_list:
        print(f'{str(node_list.index(node)):<6}{str(node):<20} -> {str(val_list[node_list.index(node)])}')
    node_idx = int(input(f'\nEnter the INDEX number of the Node to download from '))
    ## HANDLE POSSIBLE ERRORS
    try:
        if ADDR == node_list[node_idx]:
            print(f'You Selected This Very Node.')
        else:
            return node_list[node_idx]
    except Exception as e:
        print(f'Error with Index: {e}')

### SELECT SPECIFIC FILES FROM A LIST OF FILES TO DOWNLOAD
def selectFilesFromList(file_list):
    ## DISPLAY LIST
    print("\nSelect files to download by index number. For multiple files seperate the index number with comma:\n")
    print(f'{"Index":<8}{"File Name":<20}')
    for f in file_list:
        print(f'{file_list.index(f):<8}{f:<20}')
    ##  SAVE USER INPUT
    li = list(map(int, input('\n').split(',')))
    dl = []
    
    ## VALIDATE INPUT
    for i in li:
        try:
            if i == file_list.index(file_list[i]):
                dl.append(file_list[i])
        # INFORM IF INVALID INPUT
        except IndexError:
            print(f'\nIndex no {i} not found!')
    
    ## SHOW THE LIST OF VALID FILES THAT WILL BE DOWNLOADED
    print(f'\nDownloading {dl}\n')
    return dl

### DOWNLOAD HANDLER (ACTIVE ONLY IF DOWNLOADING)
def downloadHandler():
    ## GET ADDRESS OF NODE TO DOWNLOAD FILES FROM
    node_addr = selectNodeFromDHT()
    if not node_addr:
        return
    ## START A CONNECTION THREAD
    node = NodeThread(addr=node_addr)
    node.start()
    ## GET REMOTE FILE LIST AND SELECT FILES TO DOWNLOAD
    file_list = node.getFileList()
    file_list = selectFilesFromList(file_list)
    ## DOWNLOAD SEQUENCE
    download_time = time.time()
    fail_list = node.download(file_list)
    ## LOOP TO TRY AND DOWNLOAD FAILED FILES AGAIN
    if fail_list:
        retry = 3
        while retry:
            print(f'\n{fail_list} failed to download, {retry} tries left')
            retry = retry - 1
            fail_list = node.download(fail_list)
            if not fail_list:
                break
    ## IF STILL ANY DOWNLOADS LEFT, INFORM USER
    if fail_list:
        print(f'{fail_list} could not be downloaded')
    ## REPORT STATS AND CLOSE HANDLER
    download_time = time.time()-download_time
    print(f'\nTotal Download Time: {download_time} seconds')
    node.disconnect()

### RUN THIS PART
if __name__ == "__main__":
    logger.info(f'{"[STARTING NODE]":<26}{ADDR}')
    ## CREATE A NODE HANDLER W.R.T DHT AND ACTIVATE SELF
    dht_sync = NodeThread()
    dht_sync.start()
    dht_sync.activate()
    ## START LISTING ON PORT
    pl = threading.Thread(target=portListener, args=())
    pl.start()
    ## USER INTERFACE
    try:
        while True:
            act = int(input(f'\nNode Active (need action):\n-1 to exit\n0 Sync with DHT Server\n1 Download from other Nodes\n'))
            
            # CASE: CLOSE PROGRAM
            if act == -1:
                break
            # CASE: SYNCHRONIZE WITH DHT
            elif act == 0:
                dht_sync.sync()
            # CASE: DOWNLOAD FILES FROM OTHER NODES AND SYNCHRONIZE
            elif act == 1:
                downloadHandler()
                dht_sync.sync()
            # CASE: ERROR
            else:
                print('wrong command try again')

    except KeyboardInterrupt:
        logger.info((f'{"[KEYBOARD INTERRUPT]":<26}Shutting Down Node'))
    ## DEACTIVATE NODE ON DHT SERVER AND CLOSE NODE HANDLER
    dht_sync.deactivate()
    dht_sync.disconnect()
    ## STOP LISTING AND SHUTDOWN
    server.shutdown(0)
    logger.info(f'{"[WARNING]":<26}{threading.activeCount() - 1} Threads Still Active')
    logger.info(f'[SHUTDOWN]')