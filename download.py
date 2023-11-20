import socket
from database import *
import mysql.connector
import os

def serve_block(file_id, block_id,file_name,data_node_id, client_socket,mysql_connection):
    block_filename = get_blockfilename(file_id,block_id,mysql_connection)
    i = 0
    try:
        with open(block_filename[0], "rb") as block_file:
            print(f"Reading {block_id}")
            while True:
                data = block_file.read()
                # print(data)
                if not data:
                    break
                client_socket.sendall(data)
                print(f"Sent {len(data)} bytes{i}")
                i = i+1
            print(f"Finished sending Block-{block_id} to DataNode-{data_node_id}")
    except FileNotFoundError:
        print(f"Block not found: {block_filename}")
        block_filename = get_replicatename(file_id,block_id,mysql_connection)
        for j in block_filename:
            try:
                with open(j[0], "rb") as block_file:
                    print(f"Reading {block_id}")
                    while True:
                        data = block_file.read()
                        # print(data)
                        if not data:
                            break
                        client_socket.sendall(data)
                        print(f"Sent {len(data)} bytes{i}")
                        i = i+1
                    print(f"Finished sending Block-{block_id} to DataNode-{data_node_id}")
                    break
            except FileNotFoundError:
                continue

def delete_block(file_id, block_id,file_name,data_node_id, client_socket,mysql_connection):
    print("DEL")
    block_filename = get_blockfilename(file_id,block_id,mysql_connection)
    if block_filename:
        if os.path.exists(block_filename[0]):
            os.remove(block_filename[0])
        delete_server_block_metadata(file_id,block_id,mysql_connection)
    block_filename = get_replicatename(file_id,block_id,mysql_connection)
    if block_filename:
        for j in block_filename:
            if os.path.exists(j[0]):
                os.remove(j[0])
        delete_server_replicate_metadata(file_id,block_id,mysql_connection)


def download_block_data():
    server_address = ('0.0.0.0', 12346)
    
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        mysql_connection = mysql.connector.connect(
                host="localhost",
                user="root",
                password="methmonk",
                database="bigdata",
            )
        s.bind(server_address)
        s.listen()

        print(f"Server listening on {server_address}")
        while True:
            conn, addr = s.accept()
            # i = 0
            with conn:
                print(f"Connection from {addr}")
                metadata = conn.recv(1024).decode().split()
                print(metadata)
                if metadata[0]=="GET_BLOCK":
                    serve_block(metadata[1],metadata[2],metadata[3],metadata[4], conn,mysql_connection)
                elif metadata[0] == "DELETE_BLOCK":
                    delete_block(metadata[1],metadata[2],metadata[3],metadata[4], conn,mysql_connection)

if __name__ == '__main__':
    download_block_data()