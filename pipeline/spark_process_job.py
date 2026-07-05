from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, concat, lit, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType

spark = (
    SparkSession.builder
    .appName("Kafka-To-Minio-Batch")
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.path.style.access", "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000")
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin")
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin")
    .getOrCreate()
)

schema = StructType([
    StructField("Date", StringType(), True),
    StructField("Time", StringType(), True),
    StructField("CO", StringType(), True),
    StructField("NO2", StringType(), True),
    StructField("Temperature", StringType(), True),
    StructField("Relative_Humidity", StringType(), True),
    StructField("Absolute_Humidity", StringType(), True)
])

df_kafka = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "kafka1:9092")
    .option("subscribe", "sensor_raw")
    .option("startingOffsets", "earliest")
    .load()
)

df_parsed = df_kafka.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

df_final = df_parsed.withColumn(
    "timestamp", 
    to_timestamp(concat(col("Date"), lit(" "), col("Time")), "dd/MM/yyyy HH.mm.ss")
).select(
    col("timestamp"),
    col("CO").cast("double"),
    col("NO2").cast("double"),
    col("Temperature").cast("double"),
    col("Relative_Humidity").cast("double"),
    col("Absolute_Humidity").cast("double")
)

output_path = "s3a://air-quality/processed/airquality/"
df_final.write.mode("overwrite").parquet(output_path)

print(f"Processamento concluído! Salvo em: {output_path}")
