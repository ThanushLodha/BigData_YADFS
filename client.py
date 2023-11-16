import socket
import os


def receive_block_data():
    # Replace with the desired server IP address and port number
    server_address = ('0.0.0.0', 12345)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(server_address)
        s.listen()

        print(f"Server listening on {server_address}")

        while True:
            conn, addr = s.accept()
            with conn:
                print(f"Connection from {addr}")

                # Receive metadata (file path, block id, data node id)
                metadata = conn.recv(1024).decode().split()
                # file_path, block_id, data_node_id = metadata

                block_filename = f"data_node_{metadata[2]}/{metadata[0]}_block{metadata[1]}.dat"
                os.makedirs(os.path.dirname(block_filename), exist_ok=True)

                # Receive block data
                with open(block_filename, "wb") as block_file:
                    while True:
                        data = conn.recv(1024)
                        if not data:
                            break
                        block_file.write(data)

                print(
                    f"Received Block-{metadata[1]} from DataNode-{metadata[2]} and saved as {block_filename}")


if __name__ == "__main__":
    receive_block_data()
