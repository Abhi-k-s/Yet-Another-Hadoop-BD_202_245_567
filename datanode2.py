import socket
import time

def datanode2HB2():
    msgFromClient = "2"
    bytesToSend = str.encode(msgFromClient)
    serverAddressPort = ("127.0.0.1", 2000)
    bufferSize = 1024
    UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)
    while True:
        UDPClientSocket.sendto(bytesToSend, serverAddressPort)
