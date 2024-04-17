"""
Contains a context manager for a database connection.

Author: Rami Pellumbi - SP24
"""
import psycopg2

DB_NAME  = 'university-db'    # same as in lab 0
USER     = 'inyoungshin'        # replace with your username if necessary (might be postgres)
PASSWORD = ''                 # replace with your password if necessary
HOST     = 'localhost'        # replace if you're running postgres on a different host
PORT     = '5432'             # default port that postgres listens on - replace if necessary

class DatabaseConnection:
    """
    This class is a context manager for a database connection. 
    It automatically establishes a connection to the database when the context is entered
    and closes the connection when the context is exited.

    Example Usage:
    ```
    with DatabaseConnection() as cursor:
        cursor.execute('SELECT * FROM students;')
        results = cursor.fetchall()
        print(results)
    ```
    """
    def __init__(self):
        self.connection = None
        self.cursor = None

    def __enter__(self):
        config = {
            'dbname': DB_NAME,
            'user': USER,
            'password': PASSWORD,
            'host': HOST,
            'port': PORT
        }
        self.connection = psycopg2.connect(**config)
        self.cursor = self.connection.cursor()
        return self.cursor

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.connection:
            self.connection.commit()
            self.connection.close()
        if self.cursor:
            self.cursor.close()

        if exc_type or exc_val or exc_tb:
            print(f'Error: {exc_type}, {exc_val}, {exc_tb}')
