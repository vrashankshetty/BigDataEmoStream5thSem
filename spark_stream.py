from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, count, window, when, ceil, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, TimestampType

schema = StructType([
    StructField("timestamp", TimestampType(), True),
    StructField("emoji_type", StringType(), True)
])

spark = SparkSession.builder \
    .appName("EmojiStreamingApp") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3") \
    .getOrCreate()
    
spark.conf.set("spark.sql.streaming.trigger.interval", "2 seconds")


df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "emojievents") \
    .load()

df_parsed = df.select(from_json(col("value").cast("string"), schema).alias("json_data"))

df_extracted = df_parsed.select(
    col("json_data.timestamp").alias("timestamp"),
    col("json_data.emoji_type").alias("emoji_type")
)

result = df_extracted \
    .withWatermark("timestamp", "2 seconds") \
    .groupBy(
        window("timestamp", "2 seconds"),
        "emoji_type"
    ) \
    .agg(count("emoji_type").alias("count"))

result_scaled = result.select(
    "window",
    "emoji_type",
    when(col("count") > 10, ceil(col("count") / 10)).otherwise(col("count")).alias("scaled_count")
)

result_to_kafka = result_scaled.select(
    to_json(struct("window", "emoji_type", "scaled_count")).alias("value")
)

query = result_to_kafka.writeStream \
    .outputMode("update") \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("topic", "aggregatedemojis") \
    .option("checkpointLocation", "./spark_logs") \
    .trigger(processingTime="2 seconds") \
    .start()


query.awaitTermination()


#spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_stream.py
