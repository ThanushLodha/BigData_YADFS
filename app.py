from flask import Flask, render_template, request
from werkzeug.utils import secure_filename
import os
import mysql.connector
import multiprocessing
from io import BytesIO
import socket

app = Flask(__name__)

def create_file_table(mysql_connection):
    with mysql_connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS file_metadata (
                id INT AUTO_INCREMENT PRIMARY KEY,
                path VARCHAR(255) NOT NULL,
                size VARCHAR(255) NOT NULL,
                block_size INT NOT NULL
            )
            """
        )
        mysql_connection.commit()

def create_block_table(mysql_connection):
    with mysql_connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS block_metadata (
                id INT AUTO_INCREMENT PRIMARY KEY,
                file_path VARCHAR(255) NOT NULL,
                block_id INT NOT NULL,
                data_node_ip VARCHAR(15) NOT NULL,
                block_directory VARCHAR(255) NOT NULL
            )
            """
        )
        mysql_connection.commit()

def create_file(file_path, file_size, block_size, mysql_connection):
    with mysql_connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO file_metadata (path, size, block_size) "
            "VALUES (%s, %s, %s)",
            (file_path, file_size, block_size)
        )
        mysql_connection.commit()

def add_block_location(file_path, block_id, data_node_ip, block_directory, mysql_connection):
    with mysql_connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO block_metadata (file_path, block_id, data_node_ip, block_directory) "
            "VALUES (%s, %s, %s, %s)",
            (file_path, block_id, data_node_ip, block_directory)
        )
        mysql_connection.commit()

def send_block_data_to_server(file_path, block_id, data, data_node_id):
    server_ip = '192.168.0.112'
    server_port = '12345'

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((server_ip, int(server_port)))

        metadata = f"{file_path} {block_id} {data_node_id}\n"
        s.sendall(metadata.encode())
        s.sendall(data)

        print(f"DataNode: Sent Block-{block_id} to Server")

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        file = request.files['file']
        if file:
            file_content = file.read()

            file_size = len(file_content)
            block_size = 128 * 1024 * 1024  # 128 MB

            mysql_connection = mysql.connector.connect(
                host="localhost",
                user="root",
                password="methmonk",
                database="bigdata",
            )

            create_file_table(mysql_connection)
            create_block_table(mysql_connection)

            filename = secure_filename(file.filename)

            create_file(filename, file_size, block_size, mysql_connection)

            num_data_nodes = 3
            data_queue = multiprocessing.Queue()
            data_nodes = []

            for i in range(num_data_nodes):
                data_node_process = multiprocessing.Process(target=data_node, args=(i + 1, data_queue))
                data_node_process.start()
                data_nodes.append(data_node_process)

            block_id = 0
            while True:
                block_data = file_content[block_id * block_size: (block_id + 1) * block_size]
                if not block_data:
                    break

                data_queue.put({"action": "store_block", "file_path": filename, "block_id": block_id, "data": block_data})
                block_id += 1

            for _ in range(num_data_nodes):
                data_queue.put({"action": "exit"})

            for data_node_process in data_nodes:
                data_node_process.join()

            return "File uploaded and processed successfully!"

    return render_template('upload.html')

def data_node(data_node_id, data_queue):
    mysql_connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="methmonk",
        database="bigdata",
    )

    while True:
        task = data_queue.get()

        if task["action"] == "exit":
            print(f"DataNode-{data_node_id} is exiting.")
            break

        action = task["action"]
        file_path = task["file_path"]
        block_id = task["block_id"]
        data = task.get("data")

        send_block_data_to_server(file_path, block_id, data, data_node_id)

if __name__ == '__main__':
    app.run(debug=True)
