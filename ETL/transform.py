from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from config.database_config import get_spark_config
from config.spark_config import Spark_connect

# 1️⃣ SparkSession với Kafka + JDBC driver
jar = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.postgresql:postgresql:42.6.0",
    "mysql:mysql-connector-java:8.0.33"
]
spark_connect = Spark_connect(
    app_name="KafkaWeatherStreamingETL",
    master_url="local[*]",
    executor_memory="2g",
    executor_cores=1,
    driver_memory="2g",
    num_executors=1,
    jar_packages=jar,
    log_level="INFO"
)

spark = spark_connect.spark


# 2️⃣ Schema JSON từ Kafka
schema = StructType([
    StructField("name", StringType()),
    StructField("sys", StructType([
        StructField("country", StringType())
    ])),
    StructField("dt", IntegerType()),
    StructField("main", StructType([
        StructField("temp", DoubleType()),
        StructField("humidity", IntegerType())
    ])),
    StructField("wind", StructType([
        StructField("speed", DoubleType())
    ]))
])

# 3️⃣ Đọc stream từ Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "vietnam,laos") \
    .option("startingOffsets", "earliest") \
    .load()

# 4️⃣ Parse JSON và flatten
df = df_raw.selectExpr("CAST(value AS STRING) as json_str", "topic") \
    .select(from_json("json_str", schema).alias("data"), "topic") \
    .select(
        col("data.name").alias("city_name"),
        col("data.sys.country").alias("country"),
        (col("data.dt").cast(TimestampType())).alias("time"),
        col("data.main.temp").alias("temperature"),
        col("data.main.humidity").alias("humidity"),
        col("data.wind.speed").alias("wind_speed"),
        col("topic")
    )

# 5️⃣ Transform: đổi country, convert Kelvin → Celsius, làm tròn 2 chữ số
df_transformed = df.withColumn(
    "country",
    when(col("country") == "VN", "VietNam")
    .when(col("country") == "LA", "Laos")
    .otherwise(col("country"))
).withColumn(
    "temperature", round(col("temperature") - 273.15, 2)
)

# 6️⃣ Lấy config DB
spark_config = get_spark_config()

# 7️⃣ Hàm foreachBatch ghi MySQL
def write_to_mysql(batch_df, batch_id):
    batch_df_mysql = batch_df.filter(col("topic") == "vietnam").drop("topic")
    if not batch_df_mysql.rdd.isEmpty():  # kiểm tra rỗng
        batch_df_mysql.write \
            .format("jdbc") \
            .option("url", spark_config["mysql"]["jdbc_url"]) \
            .option("dbtable", spark_config["mysql"]["table"]) \
            .option("user", spark_config["mysql"]["config"]["user"]) \
            .option("password", spark_config["mysql"]["config"]["password"]) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .mode("append") \
            .save()

# 8️⃣ Hàm foreachBatch ghi PostgreSQL
def write_to_postgres(batch_df, batch_id):
    batch_df_postgres = batch_df.filter(col("topic") == "laos").drop("topic")
    if not batch_df_postgres.rdd.isEmpty():
        batch_df_postgres.write \
            .format("jdbc") \
            .option("url", spark_config["postgres"]["jdbc_url"]) \
            .option("dbtable", spark_config["postgres"]["table"]) \
            .option("user", spark_config["postgres"]["config"]["user"]) \
            .option("password", spark_config["postgres"]["config"]["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .mode("append") \
            .save()

# 9️⃣ Bắt đầu streaming
mysql_query = df_transformed.writeStream \
    .foreachBatch(write_to_mysql) \
    .outputMode("append") \
    .start()

postgres_query = df_transformed.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .start()

spark.streams.awaitAnyTermination()
