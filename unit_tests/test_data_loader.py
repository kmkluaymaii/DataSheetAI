import unittest
import pandas as pd
import sqlite3
import os

from modules.data_loader import load_data, get_column_types, insert_data

class TestCSVSQLite(unittest.TestCase):

    def setUp(self):
        # Create sample DataFrame
        self.test_df = pd.DataFrame({
            "id": [1, 2],
            "name": ["Alice", "Bob"],
            "age": [21, 22],
            "gpa": [3.5, 3.8]
        })

        # Temporary DB file
        self.test_db = "test_database.db"
        self.table_name = "students"

        # Save CSV for load_data test
        self.test_csv = "test_data.csv"
        self.test_df.to_csv(self.test_csv, index=False)

    def tearDown(self):
        # Clean up files after each test
        if os.path.exists(self.test_db):
            os.remove(self.test_db)
        if os.path.exists(self.test_csv):
            os.remove(self.test_csv)

    # Test load_data
    def test_load_data(self):
        df = load_data(self.test_csv)
        self.assertEqual(len(df), 2)
        self.assertListEqual(list(df.columns), ["id", "name", "age", "gpa"])

    # Test get_column_types
    def test_get_column_types(self):
        types = get_column_types(self.test_df)
        expected = {
            "id": "INTEGER",
            "name": "TEXT",
            "age": "INTEGER",
            "gpa": "REAL"
        }
        self.assertEqual(types, expected)

    # Test insert_data
    def test_insert_data(self):
        insert_data(self.test_df, db_path=self.test_db, table_name=self.table_name)

        conn = sqlite3.connect(self.test_db)
        cursor = conn.cursor()

        # Check table exists + data inserted
        cursor.execute(f"SELECT * FROM {self.table_name}")
        rows = cursor.fetchall()

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][1], "Alice")  # name column
        self.assertEqual(rows[1][2], 22)       # age column

        conn.close()


if __name__ == "__main__":
    unittest.main()
