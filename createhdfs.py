import json
import os
from shutil import rmtree

f = open("config_sample.json")

data = json.load(f)

BLOCK_SIZE = 64
REPLICATION_FACTOR = 3
NUM_DATANODES = 5
DATANODE_SIZE = 10
SYNC_PERIOD = 180

try:
    rmtree('DATANODE')
except:
    pass
try:
    rmtree('NAMENODE')
except:
    pass

os.mkdir('DATANODE')
os.mkdir('NAMENODE')

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

for i in range(1, NUM_DATANODES+1):
    dirname = './DATANODE/datanode{}/'.format(i)
    os.mkdir(dirname)
    filename = 'DATANODE/datanode{}/datanode{}.py'.format(i, i)
    open('DATANODE/datanode{}/__init__.py'.format(i, i), 'w').close()
    open(filename, 'w').close()
    filehandle = open(filename,"w")
    filehandle.write(datanodestring.format(i, i))
    filehandle.close()
    open('__init__.py', 'w').close()
    for j in range(1, DATANODE_SIZE+1):
        filename = 'DATANODE/datanode{}/block{}.txt'.format(i, j)
        open(filename, 'w').close()
