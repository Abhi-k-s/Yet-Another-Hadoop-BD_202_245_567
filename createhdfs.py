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

for i in range(1, NUM_DATANODES+1):
    dirname = './DATANODE/datanode{}/'.format(i)
    os.mkdir(dirname)
    for j in range(1, DATANODE_SIZE+1):
        filename = 'DATANODE/datanode{}/block{}.txt'.format(i, j)
        open(filename, 'w').close()
