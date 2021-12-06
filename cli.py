from os import error
import threading
from threading import Thread
from multiprocessing import Process
import json
import sys
from functions import split
from functions import cat

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
from secondarynamenode import secnamenodereceiveheartbeat

dsthreads = {}

for i in range(1, num_datanodes + 1):
    sys.path.append(path_to_datanodes + 'datanode{}/'.format(i))
    exec("from datanode{} import datanode{}HB".format(i, i))
    exec("dsthreads['datanodehbthread{}'] = threading.Thread(target = datanode{}HB, name = 'DatanodeHBThread{}')".format(i, i, i))


namenodeHBthread = threading.Thread(target=namenodereceiveheartbeat1, name='namenodeHBthread')
secnamenodeHBthread = threading.Thread(target=secnamenodereceiveheartbeat, name='secnamenodeHBthread')
namenodeHBthread.start()
secnamenodeHBthread.start()

for i in range(1, num_datanodes + 1):
    dsthreads['datanodehbthread{}'.format(i)].start()

functionality = '''put, syntax - put <absolute path of the file>
cat, syntax - cat <filename>
ls, syntax - ls
rm,
mkdir,
rmdir,'''

while True:
    print()
    print("Enter the DFS command...")
    print(functionality)
    print()
    command  = input().split()
    if command[0] == "put":
        try:
            # print('Command', command[0])
            # print('File', command[1])
            message = split(command[1])
            print(message)
            print()
        except error as e:
            print(e)
    if command[0] == "cat":
        try:
            cat(command[1])
        except error as e:
            print(e)
