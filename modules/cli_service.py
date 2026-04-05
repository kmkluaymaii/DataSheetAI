# User will run python3 cli_service.py "user question" xxx.csv to get the SQL query output
import sqlite3
import sys

from data_loader import load_data, insert_data

if __name__ == "__main__":
    if len(sys.argv) != 3:
        print("Usage: python3 cli_service.py \"user question\" \"csv_file_path\"")
        sys.exit(1)
    user_query = sys.argv[1]
    csv_file_path = sys.argv[2]
    db_file_path = f"data/{csv_file_path.split('/')[-1].split('.')[0]}.db"
    data = load_data(csv_file_path)
    insert_data(data, db_file_path, table_name="spotify_data")
    print(f"Data loaded successfully into {db_file_path}!")
    print("User question:", user_query)
    