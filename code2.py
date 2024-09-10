import cx_Oracle
import pandas as pd

# Step 1: Set up the connection details
# Replace with your actual database details
dsn_tns = cx_Oracle.makedsn('db-host.example.com', '1521', service_name='ORCL')

# Connect using your username and password
connection = cx_Oracle.connect(user='my_username', password='my_password', dsn=dsn_tns)

# Step 2: Read the pipe-separated file
# Path to the pipe-separated text file
file_path = '/path/to/your/file.txt'
chunk_size = 10000  # Process the file in chunks of 10,000 rows

# Step 3: Create a function to insert data in batches
def insert_data_in_batches(connection, data_chunk, table_name):
    cursor = connection.cursor()
    
    # Example table with 3 columns: ID, NAME, and AGE
    query = f"INSERT INTO {table_name} (ID, NAME, AGE) VALUES (:1, :2, :3)"
    
    # Using executemany for batch inserts (efficient for large datasets)
    cursor.executemany(query, data_chunk)
    
    # Commit the transaction after each batch
    connection.commit()
    
    cursor.close()

# Step 4: Process the file in chunks and upload to Oracle table
with pd.read_csv(file_path, sep='|', chunksize=chunk_size) as reader:
    for chunk in reader:
        # Convert the dataframe chunk to a list of tuples
        data_chunk = [tuple(x) for x in chunk.values]
        
        # Insert the data into Oracle table (replace 'your_table_name' with the actual table name)
        insert_data_in_batches(connection, data_chunk, 'my_table_name')

# Step 5: Close the connection
connection.close()
