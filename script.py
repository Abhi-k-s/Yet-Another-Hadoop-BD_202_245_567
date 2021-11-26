import threading
from threading import Thread
from multiprocessing import Process
import json
import sys

#change path to this file accordingly
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


sys.path.append(path_to_datanodes)
sys.path.append(path_to_namenodes)
from namenode import namenodereceiveheartbeat1

dsthreads = {}

for i in range(1, num_datanodes + 1):
    sys.path.append(path_to_datanodes + 'datanode{}/'.format(i))
    exec("from datanode{} import datanode{}HB".format(i, i))
    exec("dsthreads['datanodehbthread{}'] = threading.Thread(target = datanode{}HB, name = 'DatanodeHBThread{}')".format(i, i, i))


namenodeHBthread = threading.Thread(target=namenodereceiveheartbeat1, name='namenodeHBthread')
namenodeHBthread.start()

for i in range(1, num_datanodes + 1):
    dsthreads['datanodehbthread{}'.format(i)].start()
