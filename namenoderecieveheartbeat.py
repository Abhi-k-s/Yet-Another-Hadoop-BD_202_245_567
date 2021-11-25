import socket
import time
import numpy as np
import threading
import datetime

BLOCK_SIZE = 64
REPLICATION_FACTOR = 3
NUM_DATANODES = 2
DATANODE_SIZE = 10
SYNC_PERIOD = 180
HEARBEAT_TIMEPERIOD = 5.0
masterset = set(range(1, NUM_DATANODES + 1))

def namenodereceiveheartbeat1():
	localIP = "127.0.0.1"
	localPort = 2000
	bufferSize = 1024
	
	msgFromServer = "Hello UDP Client"
	bytesToSend = str.encode(msgFromServer)
	UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
	UDPServerSocket.bind((localIP, localPort))
	print("UDP server up and listening")

	set1 = set()

	while(True):
		start = time.time()
		while(time.time() < start + HEARBEAT_TIMEPERIOD):
			bytesAddressPair = UDPServerSocket.recvfrom(bufferSize)
			message = int(bytesAddressPair[0].decode())
			set1.add(int(message))
		if len(set1) == NUM_DATANODES:
			print("200, All datanodes functioning", datetime.datetime.fromtimestamp(time.time()))
			set1 = set()
		else:
			faultydatanodes = masterset - set1
			print("404, Some datanodes didn't send heartbeat", faultydatanodes)
			set1 = set()
			#should write code to remove the datanode from the metadata file
			#todo
			



