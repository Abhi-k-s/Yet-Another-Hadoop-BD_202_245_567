import socket
import time

def datanode1HB1():
    msgFromClient = "1"
    bytesToSend = str.encode(msgFromClient)
    serverAddressPort = ("127.0.0.1", 2000)
    bufferSize = 1024

    UDPClientSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

    while True:
    # Send to server using created UDP socket
        UDPClientSocket.sendto(bytesToSend, serverAddressPort)
        #time.sleep(5)
        #msgFromServer = UDPClientSocket.recvfrom(bufferSize)
        #msg = "Message from Server {}".format(msgFromServer[0])
        # print(msg)


