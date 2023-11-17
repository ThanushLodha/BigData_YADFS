import socket
import os
import mysql.connector

def create_server_metadata(mysql_connection):
    with mysql_connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS server_block_metadata (
                id INT AUTO_INCREMENT PRIMARY KEY,
                file_id INT NOT NULL,
                block_id INT NOT NULL,
                block_name VARCHAR(255) NOT NULL,
                block_location VARCHAR(255) NOT NULL
            )
            """
        )
        mysql_connection.commit()

def add_block_location(file_id,block_id,block_name,block_location,mysql_connection):
    with mysql_connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO server_block_metadata (file_id, block_id, block_name, block_location) "
            "VALUES (%s, %s, %s,%s)",
            (file_id, block_id, block_name,block_location)
        )
        mysql_connection.commit()


def receive_block_data():
    server_address = ('192.168.1.169', 12345)
    mysql_connection = mysql.connector.connect(
                host="localhost",
                user="root",
                password="methmonk",
                database="bigdata",
            )
    create_server_metadata(mysql_connection)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(server_address)
        s.listen()

        print(f"Server listening on {server_address}")

        while True:
            conn, addr = s.accept()
            # i = 0
            with conn:
                print(f"Connection from {addr}")
                metadata = conn.recv(1024).decode().split()

                block_filename = f"client/data_node_{metadata[2]}/{metadata[0]}_block{metadata[1]}.dat"
                os.makedirs(os.path.dirname(block_filename), exist_ok=True)

                print("Starting to read the file")
                with open(block_filename, "wb") as block_file:
                    while True:
                        # print(f"Writing data:{i}")
                        # i = i + 1
                        data = conn.recv(1024)
                        if not data:
                            break
                        block_file.write(data)
                block_directory = os.path.abspath(block_filename)

                print(f"Received Block-{metadata[1]} from DataNode-{metadata[2]} and saved as {block_filename}")
                print(f"File path :{block_directory}")
                block_name = f"{metadata[0]}_block{metadata[1]}.dat"
                add_block_location(metadata[3],metadata[1],block_name,block_directory,mysql_connection)
                # conn.sendall(block_directory.encode())

if __name__ == "__main__":
    receive_block_data()
