import psycopg2
from psycopg2 import Error

class PostgreSQLConnect:
    def __init__(self, host, port, user, password,database):
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.database = database
        self.config = {
            "host": host,
            "port": port,
            "user": user,
            "password": password,
            "database": database
        }

        self.connection = None
        self.cursor = None

    def connect(self):
        try:
            self.connection = psycopg2.connect(**self.config)
            self.cursor = self.connection.cursor()
            print("------------connected to PostgreSQL-------------")
            return self.connection, self.cursor
        except Error as e:
            if "does not exist" in str(e):
                print(f"Database does not exist. Connecting to default 'postgres' database")
                self.config["database"] = "postgres"
                self.connection = psycopg2.connect(**self.config)
                self.cursor = self.connection.cursor()
                return self.connection, self.cursor
            raise Exception(f"----------Failed to connect to PostgreSQL: {e}----------") from e

    def close(self):
        if self.cursor:
            self.cursor.close()
            self.cursor = None
        if self.connection and not self.connection.closed:
            self.connection.close()
            print("-------------PostgreSQL close---------------")
            self.connection = None

    def __enter__(self):
        self.connect()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()