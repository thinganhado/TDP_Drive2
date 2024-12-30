import psycopg2
from psycopg2 import OperationalError
import json
import pandas as pd

class Database:
    def __init__(self, config_file='config.json'):
        """Initialize the database connection parameters from config file."""
        self.config = self.load_config(config_file)
        self.connection = None

    def load_config(self, config_file):
        """Load database configuration from config.json."""
        with open(config_file, 'r') as file:
            config = json.load(file)
        return config['database']

    def connect(self):
        """Establish the database connection."""
        try:
            self.connection = psycopg2.connect(
                user=self.config['user'],
                password=self.config['password'],
                host=self.config['host'],
                port=self.config['port'],
                dbname=self.config['dbname']
            )
            print("Connection to the PostgreSQL database successful!")
        except OperationalError as e:
            print(f"The error '{e}' occurred while connecting to the database.")
            self.connection = None

    def execute_query(self, query, data=None):
        """Execute a query on the database."""
        if self.connection is None:
            print("No database connection.")
            return None
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, data)
                self.connection.commit()
                print("Query executed successfully!")
        except OperationalError as e:
            print(f"The error '{e}' occurred while executing the query.")

    def fetch_all(self, query, data=None):
        """Fetch all records from a SELECT query."""
        if self.connection is None:
            print("No database connection.")
            return None
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, data)
                result = cursor.fetchall()
                return result
        except OperationalError as e:
            print(f"The error '{e}' occurred while fetching data.")
            return None

    def fetch_one(self, query, data=None):
        """Fetch a single record from a SELECT query."""
        if self.connection is None:
            print("No database connection.")
            return None
        
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(query, data)
                result = cursor.fetchone()
                return result
        except OperationalError as e:
            print(f"The error '{e}' occurred while fetching data.")
            return None

    def execute_many(self, query, data):
        """Execute a bulk insert query on the database."""
        if self.connection is None:
            print("No database connection.")
            return None

        try:
            with self.connection.cursor() as cursor:
                cursor.executemany(query, data)
                self.connection.commit()
                print("Bulk insert executed successfully!")
        except OperationalError as e:
            print(f"The error '{e}' occurred while executing the bulk insert.")

    def query_df(self, query, data=None):
        """Execute a query and return the result as a Pandas DataFrame."""
        if self.connection is None:
            print("No database connection.")
            return None

        try:
            df = pd.read_sql_query(query, self.connection, params=data)
            return df
        except OperationalError as e:
            print(f"The error '{e}' occurred while fetching data.")
            return None

    def close_connection(self):
        """Close the database connection."""
        if self.connection:
            self.connection.close()
            print("Database connection closed.")
        else:
            print("No connection to close.")
