#!/usr/bin/python3
import os
import csv
import uuid
import mysql.connector
from mysql.connector import errorcode

def connect_db():
    """Connect to the MySQL database server"""
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user=os.getenv('DB_USER', 'root'),  # Default to 'root' if not set
            password=os.getenv('DB_PASSWORD', '')  # Default to empty if not set
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Error connecting to MySQL: {err}")
        return None

def create_database(connection):
    """Create the database ALX_prodev if it doesn't exist"""
    cursor = connection.cursor()
    try:
        cursor.execute("CREATE DATABASE IF NOT EXISTS ALX_prodev")
        print("Database ALX_prodev created successfully")
    except mysql.connector.Error as err:
        print(f"Failed creating database: {err}")
    finally:
        cursor.close()

def connect_to_prodev():
    """Connect to the ALX_prodev database in MySQL"""
    try:
        connection = mysql.connector.connect(
            host="localhost",
            user=os.getenv('DB_USER', 'root'),
            password=os.getenv('DB_PASSWORD', ''),
            database="ALX_prodev"
        )
        return connection
    except mysql.connector.Error as err:
        print(f"Error connecting to ALX_prodev database: {err}")
        return None

def create_table(connection):
    """Create the user_data table if it doesn't exist"""
    cursor = connection.cursor()
    table_description = (
        "CREATE TABLE IF NOT EXISTS user_data ("
        "  user_id VARCHAR(36) PRIMARY KEY,"
        "  name VARCHAR(255) NOT NULL,"
        "  email VARCHAR(255) NOT NULL,"
        "  age DECIMAL(3,0) NOT NULL,"
        "  INDEX (user_id)"
        ")"
    )
    try:
        cursor.execute(table_description)
        print("Table user_data created successfully")
    except mysql.connector.Error as err:
        print(f"Failed creating table: {err}")
    finally:
        cursor.close()

def insert_data(connection, csv_file):
    """Insert data from CSV file into the database if it doesn't exist"""
    cursor = connection.cursor()
    
    # First check if table is empty
    cursor.execute("SELECT COUNT(*) FROM user_data")
    count = cursor.fetchone()[0]
    
    if count > 0:
        print("Data already exists in the table. Skipping insertion.")
        cursor.close()
        return
    
    try:
        with open(csv_file, 'r') as file:
            # Skip the header row
            next(file)
            reader = csv.reader(file)
            
            for row in reader:
                # Clean the data (remove extra quotes)
                name = row[0].strip('"')
                email = row[1].strip('"')
                age = int(row[2].strip('"'))
                
                # Generate UUID for user_id
                user_id = str(uuid.uuid4())
                
                # Insert data
                add_user = ("INSERT INTO user_data "
                            "(user_id, name, email, age) "
                            "VALUES (%s, %s, %s, %s)")
                data_user = (user_id, name, email, age)
                
                cursor.execute(add_user, data_user)
        
        connection.commit()
        print("Data inserted successfully")
    except FileNotFoundError:
        print(f"Error: File {csv_file} not found")
    except Exception as err:
        print(f"Error inserting data: {err}")
        connection.rollback()
    finally:
        cursor.close()
