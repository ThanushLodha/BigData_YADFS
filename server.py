import socket
import threading
import mysql.connector

address = ("0.0.0.0",12345)

sock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
sock.bind(address)
sock.listen(3)

print("Server is up and listening on",address)

db_configuration = {
    "user":"root",
    "password":"methmonk",
    "host":"127.0.0.1",
    "database":"bigdata_namenode",
}

db_connection = mysql.connector.connect(**db_configuration)
db_cursor = db_connection.cursor()

def handle_client(client_socket):
    while True:
        data = client_socket.recv(1024)
        if not data:
            break
        print("Received data from client:",data.decode('utf-8'))
        filename,path,size = data.decode('utf-8').split(",")
        insert_query = "INSERT INTO files (filename,path,size) VALUES (%s,%s,%s)"
        file_data = (filename,path,size)
        db_cursor.execute(insert_query,file_data)
        db_connection.commit()
    client_socket.close()

while True:
    client_socket,client_address = sock.accept()
    print("Accepted connection from",client_address)
    client_handler = threading.Thread(target=handle_client , args=(client_socket,))
    client_handler.start()