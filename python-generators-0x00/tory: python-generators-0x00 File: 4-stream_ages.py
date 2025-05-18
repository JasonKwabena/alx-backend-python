#!/usr/bin/python3
import mysql.connector
import os

def stream_user_ages():
    """Generator function that yields user ages one by one"""
    try:
        # Connect to the database
        connection = mysql.connector.connect(
            host="localhost",
            user=os.getenv('DB_USER', 'root'),
            password=os.getenv('DB_PASSWORD', ''),
            database="ALX_prodev"
        )
        
        # Create a cursor that streams results
        cursor = connection.cursor(buffered=True)
        
        # Execute the query to get just ages
        cursor.execute("SELECT age FROM user_data")
        
        # Yield ages one by one (1st loop)
        for (age,) in cursor:
            yield age
            
    except mysql.connector.Error as err:
        print(f"Database error: {err}")
        yield None
    finally:
        # Clean up
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

def calculate_average_age():
    """Calculates average age using the stream_user_ages generator"""
    total = 0
    count = 0
    
    # Process each age from the generator (2nd loop)
    for age in stream_user_ages():
        if age is not None:
            total += age
            count += 1
    
    # Calculate and return average (avoid division by zero)
    return total / count if count > 0 else 0

if __name__ == "__main__":
    average_age = calculate_average_age()
    print(f"Average age of users: {average_age:.2f}")
