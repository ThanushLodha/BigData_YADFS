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

- **Delete Directory:**
  - Endpoint: `/delete_directory`
  - Method: POST
  - Parameters: `directory_name`
  - Description: Deletes an existing directory in the distributed file system.

...

### Client Interaction

- **Upload File:**
  - Endpoint: `/upload_file`
  - Method: POST
  - Parameters: `file`
  - Description: Uploads a file to the distributed file system.

- **Download File:**
  - Endpoint: `/download_file`
  - Method: POST
  - Parameters: `file_name`
  - Description: Downloads a file from the distributed file system.

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
