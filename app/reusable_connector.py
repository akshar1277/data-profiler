import pymysql
import pyodbc
import psycopg2
class DatabaseConnector:
    def __init__(self, host,port, user, password, database):
       self.host = host
       self.port=port
       self.user = user
       self.password = password
       self.database = database
       self.connection = None
       self.cursor = None
    
    def connect(self):
        raise NotImplementedError("Subclasses must implement connect() method")
    
    def execute_query(self,query):
        try:
            self.cursor = self.connection.cursor()
            self.cursor.execute(query)
            self.connection.commit()
            return self.cursor.fetchall()
        except Exception as e:
            print(f"Error executing query: {e}")
            return None

    def disconnect(self):
        if self.connection:
            self.cursor.close()
            self.connection.close()

class MySQLConnector(DatabaseConnector):
    def connect(self):
        self.connection = pymysql.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )

class MSSQLConnector(DatabaseConnector):
    def connect(self):
        self.connection = pyodbc.connect(
            driver='{ODBC Driver 17 for SQL Server}',
            server=self.host,
            database=self.database,
            uid=self.user,
            pwd=self.password
        )

class PostgreSQLConnector(DatabaseConnector):
    def connect(self):
        self.connection = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.password,
            database=self.database
        )

# class SnowflakeConnector(DatabaseConnector):
#     def connect(self):
#         self.connection = psycopg2.connect(
#             host=self.host,
#             user=self.user,
#             password=self.password,
#             database=self.database
#         )

