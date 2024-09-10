import cx_Oracle
import pandas as pd

# Oracle connection parameters
username = 'your_username'
password = 'your_password'
dsn = 'your_host:your_port/your_service_name'

# File path
file_path = 'path_to_your_file.txt'

# Oracle table and batch size
table_name = 'your_table_name'
batch_size = 1000  # Number of rows to insert per batch

def create_connection():
    """
    Create and return Oracle DB connection
    """
    try:
        # Create Oracle connection
        connection = cx_Oracle.connect(user=username, password=password, dsn=dsn)
        print("Successfully connected to Oracle Database")
        return connection
    except cx_Oracle.DatabaseError as e:
        print(f"Error connecting to Oracle DB: {e}")
        raise

def load_data_to_oracle(file_path, connection):
    """
    Load pipe-separated data from a .txt file to an Oracle table efficiently
    """
    # Read file in chunks using Pandas
    data_iter = pd.read_csv(file_path, sep='|', chunksize=batch_size, iterator=True)
    
    cursor = connection.cursor()
    
    # Get column names from the table
    cursor.execute(f"SELECT column_name FROM all_tab_columns WHERE table_name = '{table_name.upper()}'")
    columns = [col[0] for col in cursor.fetchall()]
    columns_str = ','.join(columns)
    
    # Prepare SQL insert statement (assuming all columns are strings; adjust types if needed)
    placeholders = ','.join([f':{i+1}' for i in range(len(columns))])
    insert_sql = f"INSERT INTO {table_name} ({columns_str}) VALUES ({placeholders})"
    
    for chunk in data_iter:
        # Convert chunk to a list of tuples for bulk insert
        data = [tuple(x) for x in chunk.values]
        
        # Insert data in batch
        cursor.executemany(insert_sql, data)
        connection.commit()  # Commit after every batch to avoid excessive memory usage
    
    print("Data upload completed successfully.")

if __name__ == "__main__":
    try:
        conn = create_connection()
        load_data_to_oracle(file_path, conn)
    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        if conn:
            conn.close()
