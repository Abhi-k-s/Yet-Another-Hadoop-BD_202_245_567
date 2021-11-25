import threading
from threading import Thread
from multiprocessing import Process
from datanode1 import datanode1HB1
from datanode2 import datanode2HB2	
from namenoderecieveheartbeat import namenodereceiveheartbeat1

thread1 = threading.Thread(target=datanode1HB1, name='Thread-1')
thread2 = threading.Thread(target=datanode2HB2, name='Thread-2')
thread3 = threading.Thread(target=namenodereceiveheartbeat1, name='Thread-3')

thread3.start()
thread1.start()
thread2.start()
