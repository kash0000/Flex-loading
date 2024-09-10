import cx_Oracle
import pandas as pd

# Step 1: Set up the connection details
dsn_tns = cx_Oracle.makedsn('your_hostname', 'your_port', service_name='your_service_name')

connection = cx_Oracle.connect(user='your_username', password='your_password', dsn=dsn_tns)

# Step 2: Read the pipe-separated file
file_path = 'your_file.txt'
chunk_size = 10000  # Process the file in chunks to handle large data efficiently

# Step 3: Create a function to insert data in batches
def insert_data_in_batches(connection, data_chunk, table_name):
    cursor = connection.cursor()
    
    # Construct the insert query
    query = f"INSERT INTO {table_name} (COLUMN1, COLUMN2, COLUMN3) VALUES (:1, :2, :3)"
    
    # Using executemany for batch inserts
    cursor.executemany(query, data_chunk)
    
    # Commit the transaction after each batch
    connection.commit()
    
    cursor.close()

# Step 4: Process the file in chunks and upload to Oracle table
with pd.read_csv(file_path, sep='|', chunksize=chunk_size) as reader:
    for chunk in reader:
        # Convert the dataframe chunk to a list of tuples
        data_chunk = [tuple(x) for x in chunk.values]
        
        # Insert the data into Oracle table
        insert_data_in_batches(connection, data_chunk, 'your_table_name')

# Step 5: Close the connection
connection.close()
