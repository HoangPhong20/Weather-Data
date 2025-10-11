from databases.mysql_connect import MySQLConnect
from config.database_config import get_database_config
from databases.schema_manager import create_mysql_schema, validate_mysql_schema, create_postgresql_database, create_postgresql_schema, validate_postgresql_schema
from databases.postgresql_connect import PostgreSQLConnect

def main(config):
    # MYSQL
    with MySQLConnect(config["mysql"].host, config["mysql"].port, config["mysql"].user,
                          config["mysql"].password) as mysql_client:
        mysql_connection, mysql_cursor = mysql_client.connection, mysql_client.cursor
        create_mysql_schema(mysql_connection,mysql_cursor) # tạo schema để db có đúng cấu trúc mình muốn
        mysql_cursor.execute(
            """
            INSERT INTO weather_data (city_name, country, time, temperature, humidity, wind_speed)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            ("Hà Nội", "Vietnam", "2025-08-30 10:00:00", 30.5, 70.0, 3.2)
        )
        mysql_connection.commit()
        print("------------Inserted data to mysql------------")
        validate_mysql_schema(mysql_cursor)

    #PostgreSQL
    with PostgreSQLConnect(config["postgres"].host,config["postgres"].port,config["postgres"].user,
                           config["postgres"].password,"postgres") as postgres_client:
        # tạo db mới
        create_postgresql_database(postgres_client.cursor,postgres_client.connection,"east_asia")
        connection, cursor = create_postgresql_schema("east_asia")
        cursor.execute(
            """
            INSERT INTO weather_data (city_name, country, time, temperature, humidity, wind_speed)
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            ("Beijing", "China","2025-08-30 15:00:00", 32.1, 65.0, 2.8)
        )
        connection.commit()
        print("------------Inserted data to PostgreSQl------------")
        validate_postgresql_schema(cursor)


if __name__ == "__main__":
    config =get_database_config()
    main(config)