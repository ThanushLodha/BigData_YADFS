from flask import Flask, render_template, request,send_file
import os
from werkzeug.utils import secure_filename
import math
import multiprocessing
import time
import mysql.connector
from database import *
import socket
import requests

app = Flask(__name__)

def create_replicate_table(mysql_connection):
    with mysql_connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS replicate_metadata (
                id INT AUTO_INCREMENT PRIMARY KEY,
                file_id INT NOT NULL,
                block_id INT NOT NULL,
                data_node INT NOT NULL
            )
            """
        )
        mysql_connection.commit()
def add_replicate_location(file_id,block_id,data_node,mysql_connection):
    with mysql_connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO replicate_metadata (file_id, block_id, data_node) "
            "VALUES (%s, %s, %s)",
            (file_id, block_id, data_node)
        )
        mysql_connection.commit()
def get_replicate_id(file_id,block_id,mysql_connection):
    cursor = mysql_connection.cursor()
    query = """SELECT data_node FROM replicate_metadata WHERE file_id=%s AND block_id=%s"""
    cursor.execute(query,(file_id,block_id))
    result = cursor.fetchall()
    cursor.close()
    return result

def send_block_to_server(block_id, block_data, data_node, file_path,file_id):
    mysql_connection = mysql.connector.connect(
                host="localhost",
                user="root",
                password="mysql",
                database="bigdata",
            )
    
    api_endpoint = 'http://192.168.237.240:12345/receive_block'

    try:
        data = {'data_node': data_node , 'action':'store_data' ,'file_path' : file_path , 'block_id':block_id,'file_id':file_id}
        files = {'block_data': (f'block_{block_id}.bin', block_data)}
        response = requests.post(api_endpoint, data=data, files=files)

        if response.status_code == 200:
            print(f"Block {block_id} sent successfully to data_node {data_node}")
            add_block_location(file_id,block_id,data_node,mysql_connection)
            for i in range(3):
                if i!=data_node:
                    replicate_file(block_id,block_data,i,file_path,file_id)
        else:
            print(f'Failed to send Block {block_id} to data_node {data_node}. Status code: {response.status_code}')
    except Exception as e:
        print(f'An error occurred while sending Block {block_id} to data_node {data_node}: {str(e)}')

def replicate_file(block_id,block_data,data_node,file_path,file_id):
    mysql_connection = mysql.connector.connect(
                host="localhost",
                user="root",
                password="mysql",
                database="bigdata",
            )
    
    api_endpoint = 'http://192.168.237.240:12345/receive_block'

    try:
        data = {'data_node': data_node , 'action':'store_data' ,'file_path' : file_path , 'block_id':block_id,'file_id':file_id}
        files = {'block_data': (f'block_{block_id}.bin', block_data)}
        response = requests.post(api_endpoint, data=data, files=files)

        if response.status_code == 200:
            print(f"Block {block_id} sent successfully to data_node {data_node}")
            add_replicate_location(file_id,block_id,data_node,mysql_connection)
        else:
            print(f'Failed to send Block {block_id} to data_node {data_node}. Status code: {response.status_code}')
    except Exception as e:
        print(f'An error occurred while sending Block {block_id} to data_node {data_node}: {str(e)}')

def reconstruct_file(file_id, num_blocks,file_name,mysql_connection):
    reconstructed_data = b''
    for block_id in range(num_blocks):
        print(f"Sending block:{block_id}")
        data_node_id = int(get_node_id(file_id,block_id,mysql_connection))
        block_data = retrieve_blocks_from_server(file_id, block_id,data_node_id,file_name)
        
        if not block_data:
            data_node_id = get_replicate_id(file_id,block_id,mysql_connection)
            print(data_node_id)
            for i in data_node_id:
                block_data = retrieve_blocks_from_server(file_id, block_id,i[0],file_name)
                if block_data:
                    print("IN close")
                    reconstructed_data+=block_data
                    break
            continue
        reconstructed_data += block_data
        print(f"Received block:{block_id}")
    # print(reconstructed_data)
    if len(reconstructed_data)==0:
        print(f"Failed to retrieve Block-{block_id} from the server.")
        return None
    return reconstructed_data

def retrieve_blocks_from_server(file_id, block_id,data_node_id,file_name):
    server_ip = '192.168.237.240'
    server_port = 12346

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((server_ip, server_port))

        request_metadata = f"GET_BLOCK {file_id} {block_id} {file_name} {data_node_id}\n"
        s.sendall(request_metadata.encode())

        block_data = b""
        while True:
            data = s.recv(1024)
            # print(data)
            if not data:
                print("No data received")
                break
            block_data += data

        return block_data


@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        file = request.files['file']
        if file:
            file_content = file.read()

            file_size = len(file_content)
            block_size = 1024
            mysql_connection = mysql.connector.connect(
                host="localhost",
                user="root",
                password="mysql",
                database="bigdata",
            )
            create_file_table(mysql_connection)
            create_block_table(mysql_connection)
            create_replicate_table(mysql_connection)

            filename = secure_filename(file.filename)
            no_of_blocks = math.ceil(file_size / block_size)
            create_file(filename, file_size,no_of_blocks, mysql_connection)
            file_id  = get_file_id(filename,mysql_connection)
            # delete_file(file_id,mysql_connection)
            processes = []
            for block_id in range(no_of_blocks):
                block_data = file_content[block_id * block_size: (block_id + 1) * block_size]
                data_node = block_id % 3
                process = multiprocessing.Process(target=send_block_to_server, args=(block_id, block_data,data_node,filename,file_id))
                processes.append(process)
                process.start()

            for process in processes:
                process.join()
            # create_file(filename, file_size,no_of_blocks, mysql_connection)
            return "File uploaded and processed successfully!"

    return render_template('upload.html')

@app.route('/file_list', methods=['GET', 'POST'])
def file_list():
    mysql_connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="mysql",
        database="bigdata",
    )
    if request.method == 'POST':
        selected_file_id = request.form.get('file_id')
        if selected_file_id:
            return download_file(selected_file_id)
        else:
            return "Please select a file to download."
    
    file_list_query = """SELECT file_id, file_name FROM file_metadata"""
    cursor = mysql_connection.cursor()
    cursor.execute(file_list_query)
    file_list = cursor.fetchall()

    return render_template('file_list.html', file_list=file_list)

@app.route('/download/<file_id>')
def download_file(file_id):
    mysql_connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="mysql",
        database="bigdata",
    )
    file_id = int(file_id)  

    metadata_query = """SELECT file_name, no_of_blocks FROM file_metadata WHERE file_id=%s"""
    cursor = mysql_connection.cursor()
    cursor.execute(metadata_query, (file_id,))
    result = cursor.fetchone()
    print(result)

    if result:
        original_filename, num_blocks = result
        reconstructed_data = reconstruct_file(file_id, num_blocks,original_filename,mysql_connection)

        if reconstructed_data:
            temp_file_path = f"temp_reconstructed_file_{file_id}.dat"
            with open(temp_file_path, 'wb') as temp_file:
                temp_file.write(reconstructed_data)
            response = send_file(
                temp_file_path,
                as_attachment=True,
                download_name=original_filename,
                mimetype='application/octet-stream'
            )

            return response
        else:
            return "Failed to reconstruct and download the file."
    else:
        return "File metadata not found."

if __name__ == '__main__':
    app.run(debug=True)
