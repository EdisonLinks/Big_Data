from pyspark.ml.recommendation import ALSModel
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    desc,
    row_number,
)
from pyspark.sql.window import Window


RATINGS_PATH = "hdfs:///Data/processed/ratings_for_als"
MODEL_PATH = "hdfs:///Data/model/als_model"
OUTPUT_PATH = "hdfs:///Data/output/als_meal_similarities"
TOP_N = 10


spark = (
    SparkSession.builder
    .appName("ALSItemSimilarityRecommendation")
    .getOrCreate()
)

ratings = spark.read.parquet(RATINGS_PATH)
meal_mapping = (
    ratings
    .select(
        col("meal_index").cast("int").alias("meal_index"),
        col("meal_id"),
    )
    .distinct()
)

model = ALSModel.load(MODEL_PATH)

item_factors = (
    model.itemFactors
    .select(
        col("id").cast("int").alias("meal_index"),
        col("features"),
    )
    .join(meal_mapping, on="meal_index", how="left")
    .cache()
)

item_count = item_factors.count()
print("===== ALS Item Factors =====")
print("items:", item_count)
item_factors.show(5, truncate=False)

left_items = item_factors.select(
    col("meal_index").alias("meal_index"),
    col("meal_id").alias("meal_id"),
    col("features").alias("features_a"),
)

right_items = item_factors.select(
    col("meal_index").alias("similar_meal_index"),
    col("meal_id").alias("similar_meal_id"),
    col("features").alias("features_b"),
)

pairs = (
    left_items
    .crossJoin(right_items)
    .filter(col("meal_index") != col("similar_meal_index"))
)

# Spark SQL higher-order functions keep the vector math concise and avoid
# collecting item factors to the driver.
pairs.createOrReplaceTempView("als_item_pairs")

similarities = spark.sql("""
    SELECT
        meal_index,
        meal_id,
        similar_meal_index,
        similar_meal_id,
        aggregate(
            zip_with(features_a, features_b, (x, y) -> x * y),
            CAST(0.0 AS DOUBLE),
            (acc, x) -> acc + x
        ) AS dot_product,
        sqrt(
            aggregate(
                transform(features_a, x -> x * x),
                CAST(0.0 AS DOUBLE),
                (acc, x) -> acc + x
            )
        ) AS norm_a,
        sqrt(
            aggregate(
                transform(features_b, x -> x * x),
                CAST(0.0 AS DOUBLE),
                (acc, x) -> acc + x
            )
        ) AS norm_b
    FROM als_item_pairs
""")

similarities = (
    similarities
    .filter((col("norm_a") > 0) & (col("norm_b") > 0))
    .withColumn("similarity", col("dot_product") / (col("norm_a") * col("norm_b")))
)

window = Window.partitionBy("meal_index").orderBy(desc("similarity"))

top_similarities = (
    similarities
    .withColumn("rank_no", row_number().over(window))
    .filter(col("rank_no") <= TOP_N)
    .select(
        "meal_id",
        "similar_meal_id",
        "meal_index",
        "similar_meal_index",
        "rank_no",
        "similarity",
    )
)

top_similarities.write.mode("overwrite").parquet(OUTPUT_PATH)

print("===== ALS Meal Similarities Output =====")
print("rows:", top_similarities.count())
print("meals:", top_similarities.select("meal_index").distinct().count())
top_similarities.orderBy("meal_index", "rank_no").show(30, truncate=False)

item_factors.unpersist()
spark.stop()
