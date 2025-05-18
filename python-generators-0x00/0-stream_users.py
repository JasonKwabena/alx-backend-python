#!/usr/bin/python3
import mysql.connector
import os

def stream_users():
    """Generator function that yields rows from user_data table one by one"""
    try:
        # Connect to the database
        connection = mysql.connector.connect(
            host="localhost",
            user=os.getenv('DB_USER', 'root'),
            password=os.getenv('DB_PASSWORD', ''),
            database="ALX_prodev"
        )
        
        # Create a buffered cursor (fetches rows one at a time)
        cursor = connection.cursor(buffered=True)
        
        # Execute the query
        cursor.execute("SELECT * FROM user_data")
        
        # Yield rows one by one
        for row in cursor:
            yield row
            
    except mysql.connector.Error as err:
        print(f"Database error: {err}")
        yield None
    finally:
        # Clean up
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

# Example usage (not part of the required function)
if __name__ == "__main__":
    user_stream = stream_users()
    for i, user in enumerate(user_stream):
        if i >= 5:  # Just show first 5 for demonstration
            break
        print(user)
