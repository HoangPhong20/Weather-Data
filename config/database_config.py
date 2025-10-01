from dotenv import load_dotenv
import os
from dataclasses import dataclass

@dataclass
class MySQLConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    table: str = "weather_data"

@dataclass
class PostgresConfig:
    host: str
    port: int
    user: str
    password: str
    database: str
    table: str = "weather_data"

def get_database_config():
    load_dotenv()
    config = {
        "mysql": MySQLConfig(
            host=os.getenv("MYSQL_HOST"),
            port=int(os.getenv("MYSQL_PORT")),
            user=os.getenv("MYSQL_USER"),
            password=os.getenv("MYSQL_PASSWORD"),
            database=os.getenv("MYSQL_DATABASE"),
            table= "weather_data"
        ),
        "postgres": PostgresConfig(
            host=os.getenv("POSTGRES_HOST"),
            port=int(os.getenv("POSTGRES_PORT")),
            user=os.getenv("POSTGRES_USER"),
            password=os.getenv("POSTGRES_PASSWORD"),
            database=os.getenv("POSTGRES_DATABASE"),
            table= "weather_data"
        )
    }
    return config


def get_spark_config():
    db_config = get_database_config()

    return {
        "mysql": {
            "table": db_config["mysql"].table,
            "jdbc_url": "jdbc:mysql://{}:{}/{}".format(
                db_config["mysql"].host,
                db_config["mysql"].port,
                db_config["mysql"].database
            ),
            "config": {
                "host": db_config["mysql"].host,
                "port": db_config["mysql"].port,
                "user": db_config["mysql"].user,
                "password": db_config["mysql"].password,
                "database": db_config["mysql"].database
            }
        },

        "postgres": {
            "table": db_config["postgres"].table,
            "jdbc_url": "jdbc:postgresql://{}:{}/{}".format(
                db_config["postgres"].host,
                db_config["postgres"].port,
                db_config["postgres"].database
            ),
            "config": {
                "host": db_config["postgres"].host,
                "port": db_config["postgres"].port,
                "user": db_config["postgres"].user,
                "password": db_config["postgres"].password,
                "database": db_config["postgres"].database
            }
        }
    }
