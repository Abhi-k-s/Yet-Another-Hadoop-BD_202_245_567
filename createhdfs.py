import json
import os
from shutil import rmtree

BLOCK_SIZE = 64
REPLICATION_FACTOR = 3
NUM_DATANODES = 5
DATANODE_SIZE = 10
SYNC_PERIOD = 180

f = open("config_sample.json")
config = json.load(f)
block_size = config['block_size']
path_to_datanodes = config['path_to_datanodes']
path_to_namenodes = config['path_to_namenodes']
replication_factor = config['replication_factor']
num_datanodes = config['num_datanodes']
datanode_size = config['datanode_size']
sync_period = config['sync_period']
datanode_log_path = config['datanode_log_path']
namenode_log_path = config['namenode_log_path']
namenode_checkpoints = config['namenode_checkpoints']
fs_path = config['fs_path']
dfs_setup_config = config['dfs_setup_config']

setupfiledir = config['dfs_setup_config'][:-10]
try:
    os.mkdir(setupfiledir)
except:
    pass
setupfile = open(dfs_setup_config, 'w')
setupfile.write(str(json.dumps(config)))

try:
    rmtree(path_to_datanodes)
except:
    pass
try:
    rmtree(path_to_namenodes)
except:
    pass

os.mkdir(path_to_datanodes)
os.mkdir(path_to_namenodes)

datanodestring = '''import socket
import time

def datanode{}HB():
    msgFromClient = "{}"
    bytesToSend = str.encode(msgFromClient)
    serverAddressPort = ("127.0.0.1", 2000)
    bufferSize = 1024
    UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    while True:
        UDPClientSocket.sendto(bytesToSend, serverAddressPort)'''


namenodestring = '''import socket
import time
import datetime
import json
import os

f = open("/Users/vinaynaidu/DFS/setup.json")
config = json.load(f)
block_size = config['block_size']
path_to_datanodes = config['path_to_datanodes']
path_to_namenodes = config['path_to_namenodes']
replication_factor = config['replication_factor']
num_datanodes = config['num_datanodes']
datanode_size = config['datanode_size']
sync_period = config['sync_period']
datanode_log_path = config['datanode_log_path']
namenode_log_path = config['namenode_log_path']
namenode_checkpoints = config['namenode_checkpoints']
fs_path = config['fs_path']
dfs_setup_config = config['dfs_setup_config']

namenodelogs = open(namenode_log_path, 'a')
datanodelogs = open(datanode_log_path, 'a')

HEARBEAT_TIMEPERIOD = 5.0

masterset = set(range(1, num_datanodes + 1))

def namenodereceiveheartbeat1():
	namenodelogs = open(namenode_log_path, 'a')
	datanodelogs = open(datanode_log_path, 'a')
	localIP = "127.0.0.1"
	localPort = 2000
	bufferSize = 1024
	
	msgFromServer = "Hello UDP Client"
	bytesToSend = str.encode(msgFromServer)
	UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
	UDPServerSocket.bind((localIP, localPort))
	print("NAMENODE server up and listening")

	set1 = set()

	while(True):
		start = time.time()
		while(time.time() < start + HEARBEAT_TIMEPERIOD):
			bytesAddressPair = UDPServerSocket.recvfrom(bufferSize)
			message = int(bytesAddressPair[0].decode())
			set1.add(int(message))
		if len(set1) == num_datanodes:
			# print("200, All datanodes functioning", datetime.datetime.fromtimestamp(time.time()))
			# datanodelogs.write("200, All datanodes functioning at {}".format(datetime.datetime.fromtimestamp(time.time())))
			datanodelogs.write("200, All datanodes functioning at - ")
			datanodelogs.write(str(datetime.datetime.fromtimestamp(time.time())) + '\\n')
			datanodelogs.flush()
			# os.fsync(datanodelogs.fileno())
			set1 = set()
		else:
			faultydatanodes = masterset - set1
			# print("404, Some datanodes didn't send heartbeat", faultydatanodes)
			datanodelogs.write("404, Some datanodes didn't send heartbeat - ")
			datanodelogs.write(str(faultydatanodes) + '\\n')
			datanodelogs.flush()
			set1 = set()
			#should write code to remove the datanode from the metadata file
			#todo
'''



try:
    os.mkdir(path_to_namenodes)
except:
    pass
filename = path_to_namenodes + 'namenode.py'
handle = open(filename, 'w')
handle.write(namenodestring)

for i in range(1, num_datanodes + 1):
    dirname = path_to_datanodes + 'datanode{}/'.format(i)
    os.mkdir(dirname)
    filename = path_to_datanodes + 'datanode{}'.format(i) + '/datanode{}.py'.format(i)
    open(filename, 'w').close()
    filehandle = open(filename,"w")
    filehandle.write(datanodestring.format(i, i))
    filehandle.close()
    for j in range(1, datanode_size + 1):
        filename = 'DATANODE/datanode{}/block{}.txt'.format(i, j)
        filename = dirname + 'block{}.txt'.format(j)
        open(filename, 'w').close()
