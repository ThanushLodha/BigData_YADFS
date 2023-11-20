# Distributed File System Implementation

## Introduction

This project implements a distributed file system designed for efficient storage and management of large files across multiple nodes. It provides features such as file upload, download, fault tolerance, and data replication.

## Features

- **File Upload and Download:** Users can upload and download files from the distributed file system.
- **Fault Tolerance:** The system is designed to handle DataNode failures through data replication.
- **Data Replication:** Replication of data blocks across multiple DataNodes ensures high availability.
- **Client Interaction:** Interact with the distributed file system through a command-line or web interface.

## Installation

1. Clone the repository:

    ```bash
    git clone https://github.com/your-username/distributed-file-system.git
    cd distributed-file-system
    ```

2. Install the required Python packages:

    ```bash
    pip install -r requirements.txt
    ```

## Usage

### Metadata Operations

- **Create Directory:**
  - Endpoint: `/create_directory`
  - Method: POST
  - Parameters: `directory_name`
  - Description: Creates a new directory in the distributed file system.
### Metadata Operation Process Flow
Client Request: Sending an operation request to the Name Node.
Name Node Verification: Verifying the validity of the operation.
Name Node Operation: Implementing changes to the metadata based on the operation.
Response: Sending a response to the client regarding the operation's status.

- **Delete Directory:**
  - Endpoint: `/delete_directory`
  - Method: POST
  - Parameters: `directory_name`
  - Description: Deletes an existing directory in the distributed file system.
### Delete Process Flow
Identification: The client initiates a request to delete a specific file or directory from the DFS.
Name Node Verification: The Name Node verifies the existence of the file or directory being deleted.
Operation Validation: If the file or directory exists, the deletion operation proceeds; otherwise, it's terminated as invalid.
Metadata Update: The Name Node updates its metadata to reflect the removal of the file or directory.
Replication Update: If applicable, changes in replication factor or block removal for fault tolerance.
Namespace Resolution: Adjustment in the namespace to reflect the deleted file or directory.
Client Acknowledgment: Acknowledgment of successful deletion from the Name Node
...

### Client Interaction

- **Upload File:**
  - Endpoint: `/upload_file`
  - Method: POST
  - Parameters: `file`
  - Description: Uploads a file to the distributed file system.
#### Upload Process Flow
File Splitting: The file is divided into fixed-size blocks.
Block Creation: Each block is assigned a unique identifier.
Uploading Blocks: Blocks are uploaded to Data Nodes, distributing them across the network.
Replication: Replicas are created for fault tolerance.
Metadata Update: Metadata is updated to store information about block locations.
Namespace Resolution: Determining where each block should be stored.
Client Acknowledgment: Progress tracking via acknowledgments from Data Nodes.
- **Download File:**
  - Endpoint: `/download_file`
  - Method: POST
  - Parameters: `file_name`
  - Description: Downloads a file from the distributed file system.
### Download Process Flow
Client Request: Initiating a request to download a file from the DFS.
Metadata Retrieval: Retrieving information about the file's structure and block locations.
Block Location Retrieval: Identifying the locations of the data blocks.
Data Block Retrieval: Retrieving data blocks from respective Data Nodes.
Data Transfer: Data transfer from Data Nodes to the client.
Reassembly: Reconstructing the original file from downloaded blocks.
File Completion Check: Validating successful retrieval of all data blocks.
Cleanup: Optionally deleting temporary data and connections to Data Nodes.
...

## Configuration

- **Name Node Configuration:**
  - The name node service is assumed to have 100% uptime.

- **Data Node Configuration:**
  - Data nodes are responsible for data storage and handling read/write operations.

- **Fault Tolerance:**
  - DataNode failures are handled by maintaining multiple replicas of data blocks.

## Acknowledgments

- Special thanks to Cloud-computing-Big-data PES University for giving us an opportunity to make a system like this.
