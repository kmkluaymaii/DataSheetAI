import pandas as pd
import sqlite3

# Function to read the csv file
def load_data(file_path):
    data = pd.read_csv(file_path)
    return data

# Function to get the type of each column to create a schema for the database
def get_column_types(data):
    types = {}
    for col in data.columns:
        data_type = str(data[col].dtype)
        if 'int' in data_type:
            types[col] = 'INTEGER'
        elif 'float' in data_type:
            types[col] = 'REAL'
        else:
            types[col] = 'TEXT' 
    return types
    

# Insert the data into the SQLite database
def insert_data(data, db="database.db", table_name="table"):
    # Get column types from the csv data
    column_types = get_column_types(data)
    
    
    