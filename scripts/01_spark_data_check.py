from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, countDistinct, desc, isnan

spark = SparkSession.builder \
    .appName("MenuRecommendationDataCheck") \
    .getOrCreate()

input_path = "hdfs:///Data/user_meal_rating_cleaned.csv"

df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .csv(input_path)

print("===== Schema =====")
df.printSchema()

print("===== Row Count =====")
print(df.count())

print("===== Basic Counts =====")
df.select(
    countDistinct("user_id").alias("user_count"),
    countDistinct("meal_id").alias("meal_count"),
    count("*").alias("rating_count")
).show()

print("===== Rating Distribution =====")
df.groupBy("rating").count().orderBy("rating").show()

print("===== Duplicate user_id + meal_id Count =====")
dup_count = df.groupBy("user_id", "meal_id") \
    .count() \
    .filter(col("count") > 1) \
    .count()
print(dup_count)

print("===== Top 10 Active Users =====")
df.groupBy("user_id").count().orderBy(desc("count")).show(10, truncate=False)

print("===== Top 10 Popular Meals =====")
df.groupBy("meal_id").count().orderBy(desc("count")).show(10, truncate=False)

spark.stop()