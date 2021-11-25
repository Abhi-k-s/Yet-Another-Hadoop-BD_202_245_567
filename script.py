import threading
from threading import Thread
from multiprocessing import Process
from namenoderecieveheartbeat import namenodereceiveheartbeat1

BLOCK_SIZE = 64
REPLICATION_FACTOR = 3
NUM_DATANODES = 5
DATANODE_SIZE = 10
SYNC_PERIOD = 180

dsthreads = {}

for i in range(1, NUM_DATANODES + 1):
    exec("from DATANODE.datanode{}.datanode{} import datanode{}HB".format(i, i, i))
    exec("dsthreads['datanodehbthread{}'] = threading.Thread(target = datanode{}HB, name = 'DatanodeHBThread{}')".format(i, i, i))


namenodeHBthread = threading.Thread(target=namenodereceiveheartbeat1, name='namenodeHBthread')
namenodeHBthread.start()

for i in range(1, NUM_DATANODES + 1):
    dsthreads['datanodehbthread{}'.format(i)].start()
