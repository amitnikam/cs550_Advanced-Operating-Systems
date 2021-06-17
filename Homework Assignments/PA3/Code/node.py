### DEFAULT PYTHON 3.8.3 MODULES
import socket
import threading
import pickle
import argparse
import logging
import time
import os
import hashlib
import random

### Code to Pass Arguments to Server Script through Linux Terminal
parser = argparse.ArgumentParser(description = "This is a distributed node in the P2P Architecture!")
parser.add_argument('--ip', metavar = 'ip', type = str, nargs = '?', default = socket.gethostbyname(socket.gethostname()))
parser.add_argument('--port', metavar = 'port', type = int, nargs = '?', default = 9000)
parser.add_argument('--dir', metavar = 'dir', type = str, nargs = '?', default = './hosted_files')
parser.add_argument('--T', metavar = 'T', type = bool, nargs = '?', default = False)
args = parser.parse_args()

### MAKE DIRECTORY TO LOG OUTPUTS TO(IF NOT MADE)
if not os.path.exists('./logs'):
    os.makedirs('./logs')
    print(f'{"[SETUP]":<26}./logs directory created.')

### MAKE DIRECTORY TO HOST FILES FROM(IF NOT MADE)
dir_loc = f'{args.dir}/{args.port}'
if not os.path.exists(dir_loc):
    os.makedirs(dir_loc)
    print(f'{"[SETUP]":<26}{dir_loc} directory created. Keep files which you want to host here.')

### SETUP LOGGING WITH DYNAMIC NAME
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s:%(message)s')
file_handler = logging.FileHandler(f'./logs/Node-{args.port}.log')
file_handler.setFormatter(formatter)
# stream_handler = logging.StreamHandler()
# stream_handler.setFormatter(formatter)
# logger.addHandler(stream_handler)
logger.addHandler(file_handler)


### CONNECTION PROTOCOL & ADDRESSES
HEADER = 16                  # Size of header
PACKET = 2048                # Size of a packet, multiple packets are sent if message is larger than packet size. 
FORMAT = 'utf-8'             # Message format
ADDR = (args.ip, args.port)  # Address socket server will bind to
MAX_CONN = 4                 # Maximum connections to DHT
TOTAL_CONN = 0               # Current connections 
LEADER = False               # Leader Status
LEADER_TIME = None           # Record leader time
DHT_ADDR = None              # Address of DHT Node
NODE_LIST = []               # List of Nodes in Network
dht = {}                     # To save DHT Record (if leader)
TEST_START = False           # For testing purpose

### DEFAULT MESSAGES
REQ_FILE_LIST_MESSAGE = "!FILE_LIST"
RES_FILE_LIST_MESSAGE = "!RES_FILE_LIST"
REQ_FILE_SRC_MESSAGE = "!REQ_FILE_SRC_MESSAGE"
RES_FILE_SRC_MESSAGE = "!RES_FILE_SRC_MESSAGE"
DOWNLOAD_MESSAGE = "!DOWNLOAD"
RES_DOWNLOAD_MESSAGE = "!RES_DOWNLOAD"
DISCONNECT_MESSAGE = "!DISCONNECT"
LEADER_CHECK = "!LEADER_CHECK"
RES_LEADER_CHECK = "!RES_LEADER_CHECK"
UPDATE_LEADER = "!UPDATE_LEADER"
UPDATE_DHT = "!UPDATE_DHT"
RES_UPDATE_DHT = "!RES_UPDATE_DHT"
DEACTIVE_NODE = "!DEACTIVE_NODE"
TEST_MESSAGE = "!TEST_MESSAGE"

### DISTRIBUTED HASH TABLE (ONLY USED WHEN LEADER)
class DHT:
    
    ## CONSTRUCTOR
    def __init__(self):
        self.data = {}
    
    ## RETURN SOURCES FOR A FILE
    def sourceList(self, file_name):
        return self.data[file_name]

    ## RETURN COMPLETE LIST OF FILES
    def fileList(self):
        file_list = [] 
        for key in self.data.keys(): 
            file_list.append(key)
        return file_list

    ## UPDATE RECORD
    def update(self, addr, file_list):
        for f in file_list:
            if f in self.data.keys(): 
                if addr not in self.data[f]:
                    self.data[f].append(addr)
            else:
                self.data[f] = []
                self.data[f].append(addr)

    ## DELETE RECORD
    def delete(self, addr):
        chk = []
        for f in self.data:
            if addr in self.data[f]:
                self.data[f].remove(addr)
                chk.append(f)
        
        for f in chk:
            if not self.data[f]:
                self.data.pop(f)

### CONNECTION HANDLER THREAD
class ConnThread(threading.Thread):

    ## CONSTRUCTOR (THREAD A NEW OR EXISTING CONNECTION)
    def __init__(self, conn=None, addr=DHT_ADDR, track=False):
        threading.Thread.__init__(self)
        # Track active connections
        self.track = track
        if self.track == True:
            global TOTAL_CONN
            TOTAL_CONN += 1
        
        # Outward Connection
        self.addr = addr
        if conn is None:
            self.conn = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.conn.settimeout(3)
            self.conn.connect(self.addr)
            self.conn.settimeout(None)
            logger.info(f'{"[NEW CONNECTION OUT]":<26}{self.addr}')
        
        # Inward Connection
        else:
            self.conn = conn
            logger.info(f'{"[NEW CONNECTION IN]":<26}{self.addr}')
        
        # HANDLER PARAMETERS
        self.listen = True
        self.buffer_file_list = None
        self.buffer_file_srcs = None
        self.buffer_file_data = None
        self.buffer_down_size = None
        self.buffer_leader_check = None
        self.buffer_update_dht_status = None
        

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
        dir_loc = f'{args.dir}/{args.port}/'
        return [f for f in os.listdir(dir_loc) if os.path.isfile(os.path.join(dir_loc, f))]

    ## FUNCTION TO SAFELY DISCONNECT AND CLOSE CONNECTION
    def disconnect(self):
        self.listen = False
        msg = {'main':DISCONNECT_MESSAGE}
        self.send(msg)
        self.conn.close()
        logger.info(f'{"[DISCONNECTED]":<26}{self.addr}')

    ##
    ### DHT SERVER FUNCTIONS
    ##
    
    ## CHECK IF A NODE IS LEADER
    def leaderPing(self):
        global ADDR
        msg = {'main':LEADER_CHECK,'addr':ADDR}
        self.send(msg)
        while self.buffer_leader_check is None:
            time.sleep(.1)
        if self.buffer_leader_check:
            global DHT_ADDR
            DHT_ADDR = self.addr
            return True
        else:
            return False

    ## UPDATE LEADER ON OTHER NODE
    def updateLeader(self):
        msg = {'main':UPDATE_LEADER, 'addr':ADDR}
        self.send(msg)
        logger.info(f'{"[LEADER ELEC NOTICE]":<26}{self.addr}')

    ## FUNCTION TO ADD NODE ON TO DHT 
    def updateDHT(self):
        msg = {'main':UPDATE_DHT, 'addr':ADDR, 'file_list':self.localFileList()}
        self.send(msg)
        logger.info(f'{"[ADDING NODE TO DHT]":<26}')
        while not self.buffer_update_dht_status:
            time.sleep(.1)
        temp = self.buffer_update_dht_status
        self.buffer_update_dht_status = None
        return temp

    ## FUNCTION TO REMOVE NODE FROM DHT
    def removeFromDHT(self):
        msg = {'main':DEACTIVE_NODE, 'addr':ADDR}
        self.send(msg)
        logger.info(f'{"[REMOVE SELF FROM DHT]":<26}')

    ## FUNCTION TO GET FILE LIST FROM DHT
    def getFileList(self):
        self.send({'main':REQ_FILE_LIST_MESSAGE})
        # USE RECEIVER & BUFFER TO RECEIVE
        while self.buffer_file_list is None:
            time.sleep(.1)
        f_list = self.buffer_file_list
        self.buffer_file_list = None
        return f_list

    ## FUNCTION TO GET NODES THAT CAN PROVIDE THE FILE
    def getFileSources(self, fname):
        self.send({'main':REQ_FILE_SRC_MESSAGE, 'file_name':fname})
        # USE RECEIVER & BUFFER TO RECEIVE
        while self.buffer_file_srcs is None:
            time.sleep(.1)
        s_list = self.buffer_file_srcs
        self.buffer_file_srcs = None
        return s_list

    ##
    ### INTER NODE FUNCTIONS
    ##

    ## FUNCTION TO DOWNLOAD FILES FROM REMOTE NODE
    def download(self, d):
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
                dir_loc = f'{args.dir}/{args.port}/'
                file_mirror = open(os.path.join(dir_loc,d), 'wb')
                file_mirror.write(self.buffer_file_data['file_data'])
                file_mirror.close()
                down_file_time = time.time()-down_file_time
                logger.info(f'{"[DOWNLOAD INFO]":<26}{self.buffer_file_data["file_name"]} downloaded from {self.addr}')
                logger.info(f'{"[DOWNLOAD STAT]":<26}{self.buffer_down_size} Bytes <- {self.addr} in {down_file_time} Seconds')
                print(f'\n{d}\nmd5: {md5_mirror}\nIntegrity check pass, downloaded successfully!')
                print(f'Downloaded in {down_file_time} seconds')
                self.buffer_file_data = None
                self.buffer_down_size = None
                return True
            # DON'T SAVE IF INTEGRITY CHECK FAILS, TRY AGAIN LATER
            else:
                print(f'\n{d}\nFile integrity failures.')
                self.buffer_file_data = None
                self.buffer_down_size = None
                fail_list.append(d)
                return False
        # IF WRONG FILE, TERMINATE
        else:
            self.buffer_file_data = None
            self.buffer_down_size = None
            fail_list.append(d)
            return False


    ##
    ### RECEIVER
    ##

    def run(self):
        while self.listen:
            msg = {'main':''}
            
            # RECEIVE MESSAGE HEADER > GET LENGTH OF MESSAGE > SAVE AND DECODE FULL MESSAGE
            msg_length = self.conn.recv(HEADER)
            if not msg_length:
                msg['main'] = DISCONNECT_MESSAGE
            
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
            
            ## UPDATE DHT RECORD
            if msg['main'] == UPDATE_DHT:
                global LEADER

                # INFORM IF NOT LEADER
                if not LEADER:
                    res = {'main':RES_UPDATE_DHT, 'status':LEADER}
                    self.send(res)
                # UPDATE DHT AND ACK(IF LEADER)
                else:
                    global dht
                    dht.update(msg['addr'],msg['file_list'])
                    res = {'main':RES_UPDATE_DHT, 'status':LEADER}
                    self.send(res)
                    logger.info(f'{"[DHT UPDATED BY]":<26}{msg["addr"]}')

            ## RESPOND TO UPDATE DHT RECORD REQUEST
            if msg['main'] == RES_UPDATE_DHT:
                # SUCCESSFUL UPDATE
                if msg['status']:
                    logger.info(f'{"[DHT UPDATE DONE]":<26}')
                    self.buffer_update_dht_status = True
                
                # WRONG UPDATE ATTEMPT
                else:
                    logger.info(f'{"[DHT UPDATE FAILED]":<26}')
                    self.buffer_update_dht_status = False

            ## UPDATE LEADER AT OTHER NODE
            if msg['main'] == UPDATE_LEADER:
                global DHT_ADDR
                DHT_ADDR = msg['addr']
                logger.info(f'{"[NEW LEADER ELECTED]":<26}{DHT_ADDR}')
                while True:
                    try:
                        dht_conn = ConnThread(addr=DHT_ADDR, track=False)
                        f_list = False
                        while not f_list:
                            f_list = dht_conn.updateDHT()
                        dht_conn.disconnect()
                        break
                    except:
                        findDHT()

            # RESPOND TO LEADER CHECK
            if msg['main'] == LEADER_CHECK:
                res = {'main':RES_LEADER_CHECK, 'leader':LEADER}
                self.send(res)
                logger.info(f'{"[LEADER PING]":<26}{msg["addr"]}')
                if msg['addr'] not in NODE_LIST:
                    updateNodeList()

            # HAND RESPONSE FOR LEADER CHECK
            if msg['main'] == RES_LEADER_CHECK:
                self.buffer_leader_check = msg['leader']
                logger.info(f'{"[LEADER PING]":<26}{self.addr}')

            # CASE: REQ FOR FILE LIST, SEND DHT FILE LIST
            if msg['main'] == REQ_FILE_LIST_MESSAGE:
                if LEADER:
                    res = {'main':RES_FILE_LIST_MESSAGE, 'status':LEADER, 'file_list':dht.fileList()}
                    logger.info(f'{"[FILE LIST REQ]":<26}')
                    self.send(res)
                else:
                    res = {'main':RES_FILE_LIST_MESSAGE, 'status':LEADER}
                    logger.info(f'{"[WRONG FILE LIST REQ]":<26}')
                    self.send(res)
            
            # CASE: RES FOR A FILE LIST REQUEST, SAVE IN BUFFER & HANDLE FAILURE
            if msg['main'] == RES_FILE_LIST_MESSAGE:
                if msg['status']:
                    self.buffer_file_list = msg['file_list']
                    logger.info(f'{"[FILE LIST RECEIVED]":<26}')
                else:
                    self.buffer_file_list = False
                    logger.info(f'{"[WRONG DHT NODE]":<26}')

            # CASE: REQ FOR FILE SOURCES, SEND DHT FILE SOURCES
            if msg['main'] == REQ_FILE_SRC_MESSAGE:
                if LEADER:
                    res = {'main':RES_FILE_SRC_MESSAGE, 'status':LEADER, 'src_list':dht.sourceList(msg['file_name'])}
                    logger.info(f'{"[FILE SOURCES REQ]":<26}')
                    self.send(res)
                else:
                    res = {'main':RES_FILE_SRC_MESSAGE, 'status':LEADER}
                    logger.info(f'{"[WRONG FILE SOURCES REQ]":<26}')
                    self.send(res)
            
            # CASE: RES FOR A FILE LIST REQUEST, SAVE IN BUFFER & HANDLE FAILURE
            if msg['main'] == RES_FILE_SRC_MESSAGE:
                if msg['status']:
                    self.buffer_file_srcs = msg['src_list']
                    logger.info(f'{"[FILE SOURCES RECEIVED]":<26}')
                else:
                    self.buffer_file_srcs = False
                    logger.info(f'{"[WRONG DHT NODE]":<26}')
            
            # REMOVE NODE FROM DHT
            if msg['main'] == DEACTIVE_NODE:
                dht.delete(msg['addr'])
                logger.info(f'{"[NODE REMOVED FROM DHT]":<26}{msg["addr"]}')

            # CASE: DOWNLOAD REQUEST
            if msg['main'] == DOWNLOAD_MESSAGE:
                up_time = time.time()
                # FIND FILE
                dir_loc = f'{args.dir}/{args.port}/'
                file_name = os.path.join(dir_loc, msg['file_name'])
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

            ## MESSAGE TO START THE TEST
            if msg['main'] == TEST_MESSAGE:
                global TEST_START
                TEST_START = True

            # CASE: DISCONNECTING REMOTE NODE, RELEASE CONNECTION
            if msg['main'] == DISCONNECT_MESSAGE:
                logger.info(f'{"[DISCONNECT ACK]":<26}{self.addr}')
                if self.track == True:
                    global TOTAL_CONN
                    TOTAL_CONN -= 1
                    if TOTAL_CONN == 0 and LEADER == True:
                        findDHT()
                self.listen = False
                self.conn.close()

### SELECT SPECIFIC FILES FROM A LIST OF FILES TO DOWNLOAD
def selectFileFromList(file_list):
    ## DISPLAY LIST
    print("\nSelect file to download by index number.\n")
    print(f'{"Index":<8}{"File Name":<20}')
    for f in file_list:
        print(f'{file_list.index(f):<8}{f:<20}')
    ##  SAVE USER INPUT
    li = int(input('\n'))
    ## SHOW THE FILE THAT WILL BE DOWNLOADED
    print(f'\nDownloading {file_list[li]}\n')
    return file_list[li]

### FUNC TO SCAN FOR NODES IN NETWORK
def updateNodeList():
    global NODE_LIST
    NODE_LIST = []
    ## SCAN BETWEEN 9000 AND 9099
    for i in range(100):
        if 9000 + i == args.port:
            continue
        
        try:
            n = ConnThread(addr=(args.ip,9000+i))
            if (args.ip,9000+i) not in NODE_LIST:
                NODE_LIST.append((args.ip,9000+i))
            n.disconnect()
        except:
            pass
    logger.info(f'{"[ACTIVE NODES UPDATED]":<26}{len(NODE_LIST)} Node(s) in Network')

### NOTIFY ALL NODES IN NETWORK FOR LEADER UPDATE
def notifyAll():
    for noti in NODE_LIST:
        try:
            noti = ConnThread(addr = noti)
            noti.updateLeader()
            noti.disconnect()
        except:
            continue

### LEADER ELECTION ALGORITHM
def findDHT():
    updateNodeList()
    logger.info(f'{"[FINDING DHT]":<26}')
    global LEADER
    global DHT_ADDR
    global dht
    global LEADER_TIME

    ## SELF ELECT(IF ALONE)
    if not NODE_LIST:
        if not LEADER:
            LEADER_TIME = time.time()
        LEADER = True
        DHT_ADDR = ADDR
        dht = DHT()
        logger.info(f'{"[SELF ELECTED LEADER]":<26}')
        print('This Node is Now DHT')
        return True
    ## LOOK UP NETWORK FOR LEADER AND UPDATE DHT
    else:
        for n in NODE_LIST:
            try:
                n = ConnThread(addr = n)
            except:
                continue
            n.start()
            leader_check = n.leaderPing()
            # Update dht if leader found
            if leader_check:
                if LEADER:
                    logger.info(f'{"[LEADER LOST AFTER]":<26}{time.time() - LEADER_TIME} Seconds')
                    LEADER = False
                logger.info(f'{"[FOUND DHT]":<26}{DHT_ADDR}')
                update_dht = n.updateDHT()
                n.disconnect()
                if update_dht == False:
                    return False
                return True
            n.disconnect()

    ## CONTINUE BEING LEADER (IF NO LEADER IN NETWORK) AND UPDATE NETWORK FOR LEADER
    if not LEADER:
        LEADER = True
        LEADER_TIME = time.time()
        dht = DHT()
        DHT_ADDR = ADDR
        notifyAll()
        logger.info(f'{"[ELECTED AS NEW DHT]":<26}')
        print('This Node is Now DHT')
        return True
    DHT_ADDR = ADDR
    logger.info(f'{"[RE-ELECTED AS DHT]":<26}')
    print('This Node is Still DHT')
    return True

### BIND AND START LISTENING ONTO PORT
def portListener():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind(ADDR)
    server.listen(0)
    logger.info(f'{"[LISTENING]":<26}On host:{args.ip} and Port:{args.port}')
    while True:
        if TOTAL_CONN < MAX_CONN:
            ## MULTI THREADING CONNECTIONS
            try:
                conn, addr = server.accept()
                new_node = ConnThread(conn, addr, True)
                new_node.start()
                logger.info(f'{"[ACTIVE CONNECTIONS]":<26}{TOTAL_CONN}')
            
            ## HANDLE ANY OTHER ERRORS
            except Exception as e:
                break

### MAIN APP
if __name__ == "__main__":
    logger.info(f'{"[STARTING]":<26}Node {args.port} is starting...')
    
    ## START LISTENER
    pl = threading.Thread(target=portListener, args=())
    pl.start()
    
    ## FIND LEADER
    find_dht = False
    while not find_dht:
        find_dht = findDHT()
    
    ## NORMAL USER INTERACTIVE APP
    if not args.T:
        while True:
            try:
                # CONNECT TO DHT
                n = ConnThread(addr=DHT_ADDR)
                n.start()
                # GET FILE LIST
                file_list = n.getFileList()
                if not file_list:
                    print('Not Enough Files in Network to Download')
                    n.disconnect()
                    break
                # SELECT FILE TO DOWNLOAD, GET CORRESPONDING NODES AND DOWNLOAD
                down_file = selectFileFromList(file_list)
                down_sources = n.getFileSources(down_file)
                print(f'Following download sources available, attempting download serially..\n{down_sources}')
                for src in down_sources:
                    print(f'Downloading {down_file} from {src}')
                    down_conn = ConnThread(addr=src)
                    down_conn.start()
                    down_check = down_conn.download(down_file)
                    down_conn.disconnect()
                    if down_check:
                        break
                n.disconnect()
            except:
                findDHT()
            break
    
    ## TEST CASE
    else:
        while True:
            if TEST_START:
                # CONNECT
                findDHT()
                n = ConnThread(addr=DHT_ADDR)
                n.start()
                # GET FILE LIST AND PICK ONE TO DOWNLOAD
                file_list = n.getFileList()
                down_sources = n.getFileSources(file_list[0])
                # KEEP TRYING UNTIL OVER
                src = random.randint(0,len(down_sources)-1)
                print(f'Downloading {file_list[0]} from {down_sources[src]}')
                down_conn = ConnThread(addr=down_sources[src])
                down_conn.start()
                down_check = down_conn.download(file_list[0])
                down_conn.disconnect()
                if down_check:
                    n.disconnect()
                    break
                n.removeFromDHT()
                n.disconnect()
        raise SystemExit("Done")

