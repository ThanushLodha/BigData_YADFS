from flask import Flask, request, jsonify,send_file
from werkzeug.utils import secure_filename
import os
from database import *
import mysql.connector
import socket
from threading import Thread
import io

other_system_app = Flask(__name__)

@other_system_app.route('/receive_block', methods=['POST'])
def receive_block():
    if 'block_data' not in request.files:
        return jsonify({'error': 'No block_data part'})
    mysql_connection = mysql.connector.connect(
                host="localhost",
                user="root",
                password="methmonk",
                database="bigdata",
            )
    create_server_metadata(mysql_connection)
    action = request.form['action']
    if action == 'store_data':
        block_data = request.files['block_data']
        data_node = int(request.form['data_node']) 
        file_path = request.form['file_path']
        block_id = request.form['block_id'] 
        file_id = request.form['file_id']
        block_filename = f"client/data_node_{data_node}/{file_path}_block{block_id}.dat"

        os.makedirs(os.path.dirname(block_filename), exist_ok=True)
        block_data.save(block_filename)
        block_directory = os.path.abspath(block_filename)

        print(f"Received Block-{block_id} from DataNode-{data_node} and saved as {block_filename}")
        print(f"File path :{block_directory}")
        block_name = f"{file_path}_block{block_id}.dat"
        add_block_location_server(file_id,block_id,block_name,block_directory,mysql_connection)

        return jsonify({'message': 'Block received successfully'})
    


if __name__ == '__main__':
    other_system_app.run(host='localhost', port=12345, debug=True)
