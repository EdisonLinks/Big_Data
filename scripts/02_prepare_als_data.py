from pyspark.sql import SparkSession
from pyspark.sql.functions import col, row_number
from pyspark.sql.window import Window
from pyspark.ml.feature import StringIndexer

spark = SparkSession.builder \
    .appName("PrepareALSData") \
    .getOrCreate()

input_path = "hdfs:///Data/user_meal_rating_cleaned.csv"
output_path = "hdfs:///Data/processed/ratings_for_als"

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(input_path)

df = df.select(
    col("user_id"),
    col("meal_id"),
    col("rating").cast("float"),
    col("review_time").cast("long"),
    col("review_date"),
    col("review"),
    col("review_length").cast("int")
)

window = Window.partitionBy("user_id", "meal_id").orderBy(col("review_time").desc())

latest_df = df.withColumn("rn", row_number().over(window)) \
    .filter(col("rn") == 1) \
    .drop("rn")

user_indexer = StringIndexer(
    inputCol="user_id",
    outputCol="user_index",
    handleInvalid="skip"
)

meal_indexer = StringIndexer(
    inputCol="meal_id",
    outputCol="meal_index",
    handleInvalid="skip"
)

user_model = user_indexer.fit(latest_df)
indexed_df = user_model.transform(latest_df)

meal_model = meal_indexer.fit(indexed_df)
indexed_df = meal_model.transform(indexed_df)

final_df = indexed_df.select(
    col("user_id"),
    col("meal_id"),
    col("user_index").cast("int"),
    col("meal_index").cast("int"),
    col("rating"),
    col("review_time"),
    col("review_date"),
    col("review"),
    col("review_length")
)

final_df.write.mode("overwrite").parquet(output_path)

print("===== ALS Data Prepared =====")
print("rows:", final_df.count())
print("users:", final_df.select("user_id").distinct().count())
print("meals:", final_df.select("meal_id").distinct().count())

spark.stop()