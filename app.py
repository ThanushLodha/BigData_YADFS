from flask import Flask, render_template, request, send_file
from werkzeug.utils import secure_filename
import mysql.connector
import multiprocessing
import socket
import math

app = Flask(__name__)

def create_file_table(mysql_connection):
    with mysql_connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS file_metadata (
                file_id INT AUTO_INCREMENT PRIMARY KEY,
                file_name VARCHAR(255) NOT NULL UNIQUE,
                size VARCHAR(255) NOT NULL,
                no_of_blocks INT NOT NULL
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
                file_id INT NOT NULL,
                block_id INT NOT NULL,
                data_node INT NOT NULL
            )
            """
        )
        mysql_connection.commit()

def create_file(file_name,file_size,block,mysql_connection):
    with mysql_connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO file_metadata (file_name,size,no_of_blocks) "
            "VALUES (%s, %s,%s)",
            (file_name, file_size,block)
        )
        mysql_connection.commit()

def add_block_location(file_id,block_id,data_node,mysql_connection):
    with mysql_connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO block_metadata (file_id, block_id, data_node) "
            "VALUES (%s, %s, %s)",
            (file_id, block_id, data_node)
        )
        mysql_connection.commit()

def send_block_data_to_server(file_path, block_id, data, data_node_id,file_id):
    server_ip = '192.168.0.112'
    server_port = 12345

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((server_ip, server_port))

        metadata = f"{file_path} {block_id} {data_node_id} {file_id}\n"
        s.sendall(metadata.encode())
        s.sendall(data)

        print(f"DataNode: Sent Block-{block_id} to Server")

def get_file_id(file_name,mysqlconnection):
    cursor = mysqlconnection.cursor()
    query = """SELECT file_id FROM file_metadata WHERE file_name=%s"""
    cursor.execute(query,(file_name,))
    result = cursor.fetchone()
    return result[0]

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

        file_path = task["file_path"]
        block_id = task["block_id"]
        data = task.get("data")
        file_id = get_file_id(file_path,mysql_connection)

        send_block_data_to_server(file_path, block_id, data, data_node_id,file_id)
        add_block_location(file_id , block_id,data_node_id,mysql_connection)

def get_node_id(file_id,block_id,mysql_connection):
    cursor = mysql_connection.cursor()
    query = """SELECT data_node FROM block_metadata WHERE file_id=%s AND block_id=%s"""
    cursor.execute(query,(file_id,block_id))
    result = cursor.fetchone()
    return result[0]

def reconstruct_file(file_id, num_blocks,file_name,mysql_connection):
    reconstructed_data = b''
    for block_id in range(num_blocks):
        print(f"Sending block:{block_id}")
        data_node_id = int(get_node_id(file_id,block_id,mysql_connection))
        block_data = retrieve_blocks_from_server(file_id, block_id,data_node_id,file_name)
        print(f"Received block:{block_id}")
        if not block_data:
            continue
        reconstructed_data += block_data
    print(reconstructed_data)
    if len(reconstructed_data)==0:
        print(f"Failed to retrieve Block-{block_id} from the server.")
        return None
    return reconstructed_data

def retrieve_blocks_from_server(file_id, block_id,data_node_id,file_name):
    server_ip = '192.168.0.112'
    server_port = 12345

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.connect((server_ip, server_port))

        request_metadata = f"GET_BLOCK {file_id} {block_id} {file_name} {data_node_id}\n"
        s.sendall(request_metadata.encode())

        block_data = b""
        while True:
            data = s.recv(1024)
            print(data)
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
            block_size = 1024*1024

            mysql_connection = mysql.connector.connect(
                host="localhost",
                user="root",
                password="methmonk",
                database="bigdata",
            )

            create_file_table(mysql_connection)
            create_block_table(mysql_connection)

            filename = secure_filename(file.filename)
            no_of_blocks = math.ceil(file_size/block_size)
            create_file(filename, file_size,no_of_blocks, mysql_connection)

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

            for i in range(num_data_nodes):
                data_queue.put({"action": "exit"})

            for data_node_process in data_nodes:
                data_node_process.join()

            return "File uploaded and processed successfully!"

    return render_template('upload.html')

@app.route('/file_list', methods=['GET', 'POST'])
def file_list():
    mysql_connection = mysql.connector.connect(
        host="localhost",
        user="root",
        password="methmonk",
        database="bigdata",
    )
    if request.method == 'POST':
        selected_file_id = request.form.get('file_id')
        if selected_file_id:
            return download_file(selected_file_id)
        else:
            return "Please select a file to download."
    
    # Fetch the list of files from the database
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
        password="methmonk",
        database="bigdata",
    )
    file_id = int(file_id)  # Convert to integer
    metadata_query = """SELECT file_name, no_of_blocks FROM file_metadata WHERE file_id=%s"""
    cursor = mysql_connection.cursor()
    cursor.execute(metadata_query, (file_id,))
    result = cursor.fetchone()

    if result:
        original_filename, num_blocks = result
        reconstructed_data = reconstruct_file(file_id, num_blocks,original_filename,mysql_connection)

        if reconstructed_data:
            # Save the reconstructed data to a temporary file
            temp_file_path = f"temp_reconstructed_file_{file_id}.dat"
            with open(temp_file_path, 'wb') as temp_file:
                temp_file.write(reconstructed_data)

            # Send the temporary file as a response with appropriate headers
            return send_file(
                temp_file_path,
                as_attachment=True,
                download_name=original_filename,
                mimetype='application/octet-stream'
            )
        else:
            return "Failed to reconstruct and download the file."
    else:
        return "File metadata not found."


if __name__ == '__main__':
    app.run(debug=True)
