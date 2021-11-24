import socket
import time
import numpy as np
import threading

BLOCK_SIZE = 64
REPLICATION_FACTOR = 3
NUM_DATANODES = 2
DATANODE_SIZE = 10
SYNC_PERIOD = 180
HEARBEAT_TIMEPERIOD = 5.0

def namenodereceiveheartbeat1():
	localIP = "127.0.0.1"
	localPort = 2000
	bufferSize = 1024
	prevtime = 0
	
	msgFromServer = "Hello UDP Client"
	bytesToSend = str.encode(msgFromServer)
	UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
	UDPServerSocket.bind((localIP, localPort))
	print("UDP server up and listening")

	presentset = set()
	prevset = set()

	while(True):
		bytesAddressPair = UDPServerSocket.recvfrom(bufferSize)
		message = int(bytesAddressPair[0].decode())
		presentset.add(int(message))
		if len(presentset) == NUM_DATANODES:
			presentime = int(time.time())
			if prevtime:
				difference = presentime - prevtime
				if difference < HEARBEAT_TIMEPERIOD + 1:
					print("200, All datanodes functioning, time difference in seconds -", difference)
					presentset = set()
					prevtime = presentime
				else:
					print("404, Some datanode not found ")
			else:
				prevtime = presentime

