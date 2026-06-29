from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, row_number
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, IntegerType
from pyspark.sql.window import Window

spark = (
    SparkSession.builder
    .appName("KafkaToHdfsETL")
    .enableHiveSupport()
    .getOrCreate()
)

schema = StructType([
    StructField("user_id", StringType(), True),
    StructField("meal_id", StringType(), True),
    StructField("rating", StringType(), True),
    StructField("review_time", StringType(), True),
    StructField("review_date", StringType(), True),
    StructField("review", StringType(), True),
    StructField("review_length", StringType(), True),
])

raw = (
    spark.read
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "meal_rating_stream")
    .option("startingOffsets", "earliest")
    .option("endingOffsets", "latest")
    .load()
)

parsed = (
    raw.selectExpr("CAST(value AS STRING) AS json_value")
    .select(from_json(col("json_value"), schema).alias("data"))
    .select("data.*")
)

clean = (
    parsed
    .withColumn("rating", col("rating").cast(DoubleType()))
    .withColumn("review_time", col("review_time").cast(LongType()))
    .withColumn("review_length", col("review_length").cast(IntegerType()))
    .filter(col("user_id").isNotNull())
    .filter(col("meal_id").isNotNull())
    .filter(col("rating").between(1.0, 5.0))
)

w = Window.partitionBy("user_id", "meal_id").orderBy(col("review_time").desc())

dedup = (
    clean
    .withColumn("rn", row_number().over(w))
    .filter(col("rn") == 1)
    .drop("rn")
)

parsed.write.mode("overwrite").option("header", "true").csv("hdfs:///Data/etl/raw/user_meal_rating_stream")
dedup.write.mode("overwrite").parquet("hdfs:///Data/etl/clean/user_meal_rating_cleaned")

spark.sql("CREATE DATABASE IF NOT EXISTS menu_recommendation")

spark.sql("DROP TABLE IF EXISTS menu_recommendation.dwd_user_meal_rating_clean")

spark.sql("""
CREATE EXTERNAL TABLE menu_recommendation.dwd_user_meal_rating_clean (
    user_id STRING,
    meal_id STRING,
    rating DOUBLE,
    review_time BIGINT,
    review_date STRING,
    review STRING,
    review_length INT
)
STORED AS PARQUET
LOCATION '/Data/etl/clean/user_meal_rating_cleaned'
""")

print("raw rows:", parsed.count())
print("clean rows:", dedup.count())

spark.stop()
