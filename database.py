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

def create_replicate_server_table(mysql_connection):
    with mysql_connection.cursor() as cursor:
        cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS server_replicate_metadata (
                id INT AUTO_INCREMENT PRIMARY KEY,
                file_id INT NOT NULL,
                block_id INT NOT NULL,
                block_name VARCHAR(255) NOT NULL,
                block_location VARCHAR(255) NOT NULL
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

def add_replicate_location(file_id,block_id,data_node,mysql_connection):
    with mysql_connection.cursor() as cursor:
        cursor.execute(
            "INSERT INTO replicate_metadata (file_id, block_id, data_node) "
            "VALUES (%s, %s, %s)",
            (file_id, block_id, data_node)
        )
        mysql_connection.commit()

def get_file_id(file_name,mysqlconnection):
    cursor = mysqlconnection.cursor()
    query = """SELECT file_id FROM file_metadata WHERE file_name=%s"""
    cursor.execute(query,(file_name,))
    result = cursor.fetchone()
    cursor.close()
    return result[0]

def get_node_id(file_id,block_id,mysql_connection):
    cursor = mysql_connection.cursor()
    query = """SELECT data_node FROM block_metadata WHERE file_id=%s AND block_id=%s"""
    cursor.execute(query,(file_id,block_id))
    result = cursor.fetchone()
    cursor.close()
    return result

def get_replicate_id(file_id,block_id,mysql_connection):
    cursor = mysql_connection.cursor()
    query = """SELECT data_node FROM replicate_metadata WHERE file_id=%s AND block_id=%s"""
    cursor.execute(query,(file_id,block_id))
    result = cursor.fetchall()
    cursor.close()
    return result

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

def add_block_location_server(file_id,block_id,block_name,block_location,mysql_connection):
        cursor = mysql_connection.cursor()
        cursor.execute("SELECT * FROM server_block_metadata where file_id = %s AND block_id=%s",(file_id,block_id))
        result = cursor.fetchone()
        if not result:
            cursor.execute("INSERT INTO server_block_metadata (file_id, block_id, block_name, block_location) VALUES (%s, %s, %s,%s)",(file_id, block_id, block_name,block_location))
            mysql_connection.commit()
        else:
            cursor.execute("INSERT INTO server_replicate_metadata (file_id, block_id, block_name, block_location) VALUES (%s, %s, %s,%s)",(file_id, block_id, block_name,block_location))
            mysql_connection.commit()

def get_blockfilename(file_id,block_id,mysql_connection):
    cursor = mysql_connection.cursor()
    query = """SELECT block_location FROM server_block_metadata WHERE file_id=%s AND block_id=%s"""
    cursor.execute(query,(file_id,block_id))
    result = cursor.fetchone()
    cursor.close()
    print(result)
    return result

def get_replicatename(file_id,block_id,mysql_connection):
    cursor = mysql_connection.cursor()
    query = """SELECT block_location FROM server_replicate_metadata WHERE file_id=%s AND block_id=%s"""
    cursor.execute(query,(file_id,block_id))
    result = cursor.fetchall()
    cursor.close()
    print(result)
    return result

def delete_file_client(file_id,mysql_connection):
    cursor = mysql_connection.cursor()
    query = """DELETE FROM file_metadata WHERE file_id=%s"""
    cursor.execute(query,(file_id,))
    mysql_connection.commit()
    cursor.close()

def delete_server_block_metadata(file_id,block_id,mysql_connection):
    cursor = mysql_connection.cursor()
    query = """DELETE FROM server_block_metadata where file_id=%s AND block_id=%s"""
    cursor.execute(query,(file_id,block_id))
    mysql_connection.commit()
    cursor.close()
    
def delete_server_replicate_metadata(file_id,block_id,mysql_connection):
    cursor = mysql_connection.cursor()
    query = """DELETE FROM server_replicate_metadata where file_id=%s AND block_id=%s"""
    cursor.execute(query,(file_id,block_id))
    mysql_connection.commit()
    cursor.close()

def delete_block_data(file_id,mysql_connection):
    cursor = mysql_connection.cursor()
    query = """DELETE FROM block_metadata WHERE file_id=%s"""
    cursor.execute(query,(file_id,))
    mysql_connection.commit()
    cursor.close()