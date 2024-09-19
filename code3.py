import cx_Oracle
import pandas as pd

# Step 1: Set up the connection details
dsn_tns = cx_Oracle.makedsn('db-host.example.com', '1521', service_name='ORCL')
connection = cx_Oracle.connect(user='my_username', password='my_password', dsn=dsn_tns)

# Step 2: Read the pipe-separated .dat file
file_path = '/unix/path/to/your/file.dat'
chunk_size = 10000  # Process the file in chunks of 10,000 rows

# Step 3: Create a function to insert data in batches
def insert_data_in_batches(connection, data_chunk, table_name):
    cursor = connection.cursor()

    # Example table with 32 columns
    query = f"""
    INSERT INTO {table_name} (COL1, COL2, COL3, COL4, COL5, COL6, COL7, COL8, 
                              COL9, COL10, COL11, COL12, COL13, COL14, COL15, COL16,
                              COL17, COL18, COL19, COL20, COL21, COL22, COL23, COL24,
                              COL25, COL26, COL27, COL28, COL29, COL30, COL31, COL32) 
    VALUES (:1, :2, :3, :4, :5, :6, :7, :8, 
            :9, :10, :11, :12, :13, :14, :15, :16, 
            :17, :18, :19, :20, :21, :22, :23, :24, 
            :25, :26, :27, :28, :29, :30, :31, :32)
    """

    cursor.executemany(query, data_chunk)
    connection.commit()
    cursor.close()

# Step 4: Process the file in chunks and upload to Oracle table
reader = pd.read_csv(file_path, sep='|', chunksize=chunk_size)

for chunk in reader:
    data_chunk = []
    
    # Convert each row of the chunk to a tuple, handle NaN values, and convert float to int if necessary
    for row in chunk.itertuples(index=False, name=None):
        # Replace NaN values with None (for Oracle NULLs) and convert float to int if needed
        processed_row = tuple(
            int(x) if isinstance(x, float) and x.is_integer() else None if pd.isna(x) else x
            for x in row
        )
        data_chunk.append(processed_row)
    
    # Insert the data into Oracle table (replace 'my_table_name' with the actual table name)
    insert_data_in_batches(connection, data_chunk, 'my_table_name')

# Step 5: Close the connection
connection.close()
