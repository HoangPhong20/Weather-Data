from pathlib import Path
from mysql.connector import Error
import psycopg2
from psycopg2 import Error
from config.database_config import get_database_config
from databases.postgresql_connect import PostgreSQLConnect

MYSQL_FILE_PATH = Path("..\sql\schemaMySQL.sql")
POSTGRESQL_FILE_PATH = Path("..\sql\schemaPostgre.sql")

def create_mysql_schema(mysql_connection, mysql_cursor):
    database = "vietnam"
    mysql_cursor.execute(f"DROP DATABASE IF EXISTS {database}")
    mysql_cursor.execute(f"CREATE DATABASE  {database}")
    mysql_connection.commit()
    print(f"---------Create database: {database} success------------")
    mysql_connection.database = database

    try:
        with open(MYSQL_FILE_PATH,"r") as f:
            sql_script = f.read()
            sql_commands = [cmd.strip() for cmd in sql_script.split(";") if cmd.strip()]

            for cmd in sql_commands:
                mysql_cursor.execute(cmd)
                print(f"---------Executed mysql command-------------")

        mysql_connection.commit()
    except Error as e:
        mysql_connection.rollback()
        raise Exception(f"----------Failed to create MySQL schema: {e} ------------") from e

def validate_mysql_schema(mysql_cursor):
    try:
        mysql_cursor.execute("SHOW TABLES")
        # row[0] để trả về list chứa string tiện cho kiểm tra, mysql_cursor show table trả về tuple
        table = [row[0] for row in mysql_cursor.fetchall()] #fetchall sẽ fetch dữ liệu ra từ buffer query rồi xóa
        if "weather_data" not in table:
            raise ValueError("----------Table 'weather_data' does not exist----------")

        mysql_cursor.execute("SELECT * FROM weather_data where id = 1")
        user = mysql_cursor.fetchone()
        if not user:
            raise ValueError("----------user not found-------------")
        print("-----------Validate schema in Mysql success-------------")
    except Error as e:

        raise Exception(f"----------MySQL schema validation failed: {e}----------") from e


def create_postgresql_schema(connection, cursor,database):
    try:
        # Autocommit để drop/create database
        connection.set_session(autocommit=True)
        cursor.execute("SELECT current_database();")
        cursor.execute(f"DROP DATABASE IF EXISTS {database};")
        cursor.execute(f"CREATE DATABASE {database};")
        print(f"-------Create database: {database} success------------")

        cursor.close()
        connection.close()
        # Kết nối lại DB mới vì connection ban đầu vẫn gắn với db cũ
        config = get_database_config()
        new_connection = psycopg2.connect(
            host=config["postgres"].host,
            port=config["postgres"].port,
            user=config["postgres"].user,
            password=config["postgres"].password,
            database=config["postgres"].database
        )
        new_cursor = new_connection.cursor()
        # Đọc schema và chạy lệnh
        with open(POSTGRESQL_FILE_PATH, "r", encoding="utf-8") as f:
            sql_commands = [cmd.strip() for cmd in f.read().split(";") if cmd.strip()]

        for cmd in sql_commands:
            new_cursor.execute(cmd)

        new_connection.commit()
        print("---------Executed PostgreSQL command-------------")
        return new_connection, new_cursor

    except Exception as e:
        if 'new_connection' in locals():
            new_connection.rollback()
            print("-----------Rolled back new connection-----------")
        raise Exception(f"----------Failed to create PostgreSQL schema: {e} ------------")
def validate_postgresql_schema(postgre_cursor):
    try:
        postgre_cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
        table = [row[0] for row in postgre_cursor.fetchall()]
        print("Tables:", table)

        if "weather_data" not in table:
            raise ValueError("----------Table 'weather_data' does not exist-----------")

        postgre_cursor.execute("SELECT * FROM weather_data WHERE id = 1")
        user = postgre_cursor.fetchone()
        print("Sample row:", user)
        if not user:
            raise ValueError("----------No data found with id = 1-------------")
        print("-----------PostgreSQL schema validation successful-------------")
    except Error as e:
        raise Exception (f"----------PostgreSQL schema validation failed: {e}----------") from e