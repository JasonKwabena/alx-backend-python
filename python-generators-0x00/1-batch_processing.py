#!/usr/bin/python3
import mysql.connector
import os

def stream_users_in_batches(batch_size):
    """Generator function that yields batches of rows from user_data table"""
    try:
        # Connect to the database
        connection = mysql.connector.connect(
            host="localhost",
            user=os.getenv('DB_USER', 'root'),
            password=os.getenv('DB_PASSWORD', ''),
            database="ALX_prodev"
        )
        
        # Create a cursor with specified batch size
        cursor = connection.cursor(buffered=True)
        
        # Execute the query
        cursor.execute("SELECT * FROM user_data")
        
        while True:
            # Fetch a batch of rows
            batch = cursor.fetchmany(batch_size)
            if not batch:
                break
            yield batch
            
    except mysql.connector.Error as err:
        print(f"Database error: {err}")
        yield None
    finally:
        # Clean up
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

def batch_processing(batch_size):
    """Generator function that processes batches to filter users over age 25"""
    # Get the batch stream generator
    batch_stream = stream_users_in_batches(batch_size)
    
    # Loop through batches (1st loop)
    for batch in batch_stream:
        if batch is None:
            continue
            
        # Process each user in batch (2nd loop)
        filtered_users = []
        for user in batch:
            # user structure: (user_id, name, email, age)
            if user[3] > 25:  # Check age > 25
                filtered_users.append(user)
        
        yield filtered_users

# Example usage (not part of the required functions)
if __name__ == "__main__":
    # Test with batch size of 10
    batch_size = 10
    
    # Process and display filtered batches (3rd loop)
    for i, filtered_batch in enumerate(batch_processing(batch_size)):
        print(f"\nBatch {i+1} - Users over 25:")
        for user in filtered_batch:
            print(f"  {user[1]} (Age: {user[3]})")
        
        # Just show first 2 batches for demonstration
        if i >= 1:
            break
