from pyspark.sql.functions import from_json, col, when, round
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, IntegerType, TimestampType
from config.database_config import get_spark_config
from config.spark_config import Spark_connect

# SparkSession với Kafka + JDBC driver
jar = [
    "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0",
    "org.postgresql:postgresql:42.6.0",
    "mysql:mysql-connector-java:8.0.33"
]
spark_connect = Spark_connect(
    app_name="KafkaWeatherStreamingETL",
    master_url="local[*]",
    executor_memory="4g",
    executor_cores=2,
    driver_memory="4g",
    num_executors=4,
    jar_packages=jar,
    log_level="INFO"
)

spark = spark_connect.spark
# Schema JSON từ Kafka
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

# Đọc stream từ Kafka
df_raw = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "southeast_asia,east_asia") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# Parse JSON và flatten
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

# Transform
df_transformed = df.withColumn(
    "country",
    when(col("country") == "VN", "Vietnam")
    .when(col("country") == "LA", "Laos")
    .when(col("country") == "TH", "Thailand")
    .when(col("country") == "KH", "Cambodia")
    .when(col("country") == "MY", "Malaysia")
    .when(col("country") == "SG", "Singapore")
    .when(col("country") == "PH", "Philippines")
    .when(col("country") == "ID", "Indonesia")
    .when(col("country") == "CN", "China")
    .when(col("country") == "JP", "Japan")
    .when(col("country") == "KR", "South Korea")
    .when(col("country") == "TW", "Taiwan")
    .when(col("country") == "HK", "Hong Kong")
    .when(col("country") == "MO", "Macau")
    .otherwise(col("country"))
).withColumn(
    "temperature", round(col("temperature") - 273.15, 2)
)
# Lấy config DB
spark_config = get_spark_config()

# Hàm foreachBatch ghi MySQL
def write_to_mysql(batch_df,batch_id):
    batch_df_mysql = batch_df.filter(col("topic") == "southeast_asia").drop("topic").coalesce(2)
    if not batch_df_mysql.rdd.isEmpty():  # kiểm tra rỗng
        batch_df_mysql.write \
            .format("jdbc") \
            .option("url", spark_config["mysql"]["jdbc_url"]) \
            .option("dbtable", spark_config["mysql"]["table"]) \
            .option("user", spark_config["mysql"]["config"]["user"]) \
            .option("password", spark_config["mysql"]["config"]["password"]) \
            .option("driver", "com.mysql.cj.jdbc.Driver") \
            .option("batchsize", 500) \
            .mode("append") \
            .save()

# Hàm foreachBatch ghi PostgreSQL
def write_to_postgres(batch_df,batch_id):
    batch_df_postgres = batch_df.filter(col("topic") == "east_asia").drop("topic").coalesce(2)
    if not batch_df_postgres.rdd.isEmpty():
        batch_df_postgres.write \
            .format("jdbc") \
            .option("url", spark_config["postgres"]["jdbc_url"]) \
            .option("dbtable", spark_config["postgres"]["table"]) \
            .option("user", spark_config["postgres"]["config"]["user"]) \
            .option("password", spark_config["postgres"]["config"]["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .option("batchsize", 500) \
            .mode("append") \
            .save()

# --- Ghi dữ liệu đến MySQL ---
mysql_query = df_transformed.writeStream \
    .foreachBatch(write_to_mysql) \
    .outputMode("append") \
    .queryName("WriteToMySQL") \
    .option("checkpointLocation", "/tmp/checkpoint/mysql") \
    .trigger(processingTime="10 seconds") \
    .start()

# --- Ghi dữ liệu đến PostgreSQL ---
postgres_query = df_transformed.writeStream \
    .foreachBatch(write_to_postgres) \
    .outputMode("append") \
    .queryName("WriteToPostgres") \
    .option("checkpointLocation", "/tmp/checkpoint/postgres") \
    .trigger(processingTime="30 seconds") \
    .start()

spark.streams.awaitAnyTermination()
