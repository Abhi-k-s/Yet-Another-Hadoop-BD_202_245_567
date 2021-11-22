import socket
import time

localIP = "127.0.0.1"
localPort = 2000
bufferSize = 1024

msgFromServer = "Hello UDP Client"
bytesToSend = str.encode(msgFromServer)

# Create a datagram socket
UDPServerSocket = socket.socket(family=socket.AF_INET, type=socket.SOCK_DGRAM)

 

# Bind to address and ip
UDPServerSocket.bind((localIP, localPort))
print("UDP server up and listening")

 

# Listen for incoming datagrams
lis = []
while(True):
    print("entered loop")
    bytesAddressPair = UDPServerSocket.recvfrom(bufferSize)
    message = bytesAddressPair[0]
    print("recevin msg")
    lis.append(message)
    if message == "1":
        print("datanode1", time.time())

    elif message == "2":
        print("datanode2", time.time())

    address = bytesAddressPair[1]
    clientMsg = "Message from Client:{}".format(message)
    clientIP  = "Client IP Address:{}".format(address)
    
    # print(clientMsg)
    # print(clientIP)
    # Sending a reply to client

    UDPServerSocket.sendto(bytesToSend, address)