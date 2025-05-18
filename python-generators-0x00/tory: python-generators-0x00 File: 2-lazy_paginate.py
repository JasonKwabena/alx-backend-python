#!/usr/bin/python3
import mysql.connector
import os

def paginate_users(page_size, offset):
    """Fetch a specific page of users from the database"""
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user=os.getenv('DB_USER', 'root'),
            password=os.getenv('DB_PASSWORD', ''),
            database="ALX_prodev"
        )
        
        cursor = connection.cursor(dictionary=True)
        query = "SELECT * FROM user_data LIMIT %s OFFSET %s"
        cursor.execute(query, (page_size, offset))
        
        page = cursor.fetchall()
        return page
        
    except mysql.connector.Error as err:
        print(f"Database error: {err}")
        return []
    finally:
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()

def lazy_paginate(page_size):
    """Generator that yields pages of users one at a time, fetching only when needed"""
    offset = 0
    while True:  # This is our single loop
        page = paginate_users(page_size, offset)
        if not page:
            break
        yield page
        offset += page_size

# Example usage
if __name__ == "__main__":
    # Create the lazy paginator with page size of 5
    paginator = lazy_paginate(5)
    
    # Get and display first page
    print("First Page:")
    first_page = next(paginator)
    for user in first_page:
        print(f"  {user['name']} (Age: {user['age']})")
    
    # Get and display second page
    print("\nSecond Page:")
    second_page = next(paginator)
    for user in second_page:
        print(f"  {user['name']} (Age: {user['age']})")
