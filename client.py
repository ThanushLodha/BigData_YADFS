import socket

server_address = ('127.0.0.1',12345)

client_socket = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
client_socket.connect(server_address)

metadata = "example.txt,/mydata/documents,1024"
client_socket.sendall(metadata.encode('utf-8'))

data = client_socket.recv(1024)
print("Received response from the server:",data.decode('utf-8'))

client_socket.close()