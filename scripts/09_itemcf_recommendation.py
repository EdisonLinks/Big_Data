from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    abs as spark_abs,
    avg,
    col,
    count,
    desc,
    lit,
    row_number,
    sqrt,
    sum as spark_sum,
)
from pyspark.sql.window import Window


RATINGS_PATH = "hdfs:///Data/processed/ratings_for_als"
SIMILARITY_OUTPUT_PATH = "hdfs:///Data/output/itemcf_meal_similarities"
RECOMMEND_OUTPUT_PATH = "hdfs:///Data/output/itemcf_user_recommendations"
METRICS_OUTPUT_PATH = "hdfs:///Data/output/itemcf_metrics"

TOP_SIMILAR_N = 30
TOP_RECOMMEND_N = 10
MIN_COMMON_USERS = 2
SEED = 2026


spark = (
    SparkSession.builder
    .appName("ItemCFRecommendation")
    .getOrCreate()
)

ratings = (
    spark.read.parquet(RATINGS_PATH)
    .select(
        col("user_id"),
        col("meal_id"),
        col("user_index").cast("int").alias("user_index"),
        col("meal_index").cast("int").alias("meal_index"),
        col("rating").cast("float").alias("rating"),
    )
    .dropna(subset=["user_index", "meal_index", "rating"])
    .cache()
)

print("===== ItemCF Input Data =====")
print("rows:", ratings.count())
print("users:", ratings.select("user_index").distinct().count())
print("meals:", ratings.select("meal_index").distinct().count())

train_data, test_data = ratings.randomSplit([0.8, 0.2], seed=SEED)
train_data = train_data.cache()
test_data = test_data.cache()

train_count = train_data.count()
test_count = test_data.count()
print("train rows:", train_count)
print("test rows:", test_count)

item_norms = (
    train_data
    .groupBy("meal_index")
    .agg(sqrt(spark_sum(col("rating") * col("rating"))).alias("norm"))
)

left_ratings = train_data.select(
    col("user_index"),
    col("meal_index").alias("meal_index"),
    col("rating").alias("rating_a"),
)

right_ratings = train_data.select(
    col("user_index"),
    col("meal_index").alias("similar_meal_index"),
    col("rating").alias("rating_b"),
)

co_ratings = (
    left_ratings
    .join(right_ratings, on="user_index")
    .filter(col("meal_index") != col("similar_meal_index"))
)

similarities = (
    co_ratings
    .groupBy("meal_index", "similar_meal_index")
    .agg(
        spark_sum(col("rating_a") * col("rating_b")).alias("dot_product"),
        count(lit(1)).alias("common_users"),
    )
    .filter(col("common_users") >= MIN_COMMON_USERS)
    .join(item_norms.select(col("meal_index"), col("norm").alias("norm_a")), on="meal_index")
    .join(
        item_norms.select(
            col("meal_index").alias("similar_meal_index"),
            col("norm").alias("norm_b"),
        ),
        on="similar_meal_index",
    )
    .filter((col("norm_a") > 0) & (col("norm_b") > 0))
    .withColumn("similarity", col("dot_product") / (col("norm_a") * col("norm_b")))
)

similarity_window = Window.partitionBy("meal_index").orderBy(
    desc("similarity"),
    desc("common_users"),
)

top_similarities = (
    similarities
    .withColumn("rank_no", row_number().over(similarity_window))
    .filter(col("rank_no") <= TOP_SIMILAR_N)
    .select(
        "meal_index",
        "similar_meal_index",
        "rank_no",
        "similarity",
        "common_users",
    )
    .cache()
)

meal_mapping = ratings.select("meal_index", "meal_id").distinct()

top_similarities_with_id = (
    top_similarities
    .join(meal_mapping, on="meal_index", how="left")
    .join(
        meal_mapping.select(
            col("meal_index").alias("similar_meal_index"),
            col("meal_id").alias("similar_meal_id"),
        ),
        on="similar_meal_index",
        how="left",
    )
    .select(
        "meal_id",
        "similar_meal_id",
        "meal_index",
        "similar_meal_index",
        "rank_no",
        "similarity",
        "common_users",
    )
)

top_similarities_with_id.write.mode("overwrite").parquet(SIMILARITY_OUTPUT_PATH)

rated_pairs = train_data.select("user_index", "meal_index").distinct()

candidate_scores = (
    train_data
    .select("user_index", "meal_index", "rating")
    .join(top_similarities, on="meal_index")
    .withColumn("weighted_score", col("rating") * col("similarity"))
    .groupBy("user_index", "similar_meal_index")
    .agg(
        spark_sum("weighted_score").alias("score_sum"),
        spark_sum("similarity").alias("similarity_sum"),
        count(lit(1)).alias("evidence_count"),
    )
    .filter(col("similarity_sum") > 0)
    .withColumn("predicted_score", col("score_sum") / col("similarity_sum"))
    .withColumnRenamed("similar_meal_index", "meal_index")
    .join(rated_pairs, on=["user_index", "meal_index"], how="left_anti")
)

recommend_window = Window.partitionBy("user_index").orderBy(
    desc("predicted_score"),
    desc("evidence_count"),
)

user_mapping = ratings.select("user_index", "user_id").distinct()

top_recommendations = (
    candidate_scores
    .withColumn("rank_no", row_number().over(recommend_window))
    .filter(col("rank_no") <= TOP_RECOMMEND_N)
    .join(user_mapping, on="user_index", how="left")
    .join(meal_mapping, on="meal_index", how="left")
    .select(
        "user_id",
        "meal_id",
        "user_index",
        "meal_index",
        "rank_no",
        "predicted_score",
        "evidence_count",
    )
)

top_recommendations.write.mode("overwrite").parquet(RECOMMEND_OUTPUT_PATH)

global_mean = train_data.agg(avg("rating").alias("global_mean")).first()["global_mean"]

prediction_source = candidate_scores.select(
    "user_index",
    "meal_index",
    "predicted_score",
)

test_predictions = (
    test_data
    .join(prediction_source, on=["user_index", "meal_index"], how="inner")
    .withColumn("squared_error", (col("rating") - col("predicted_score")) ** 2)
    .withColumn("absolute_error", spark_abs(col("rating") - col("predicted_score")))
)

prediction_rows = test_predictions.count()

if prediction_rows:
    metric_values = test_predictions.agg(
        sqrt(avg("squared_error")).alias("rmse"),
        avg("absolute_error").alias("mae"),
    ).first()
    rmse = float(metric_values["rmse"])
    mae = float(metric_values["mae"])
else:
    rmse = None
    mae = None

metrics = spark.createDataFrame(
    [(
        "itemcf",
        TOP_SIMILAR_N,
        MIN_COMMON_USERS,
        test_count,
        prediction_rows,
        float(prediction_rows) / float(test_count) if test_count else None,
        rmse,
        mae,
    )],
    [
        "model_type",
        "top_similar_n",
        "min_common_users",
        "test_rows",
        "prediction_rows",
        "prediction_coverage",
        "rmse",
        "mae",
    ],
)

metrics.write.mode("overwrite").option("header", "true").csv(METRICS_OUTPUT_PATH)

print("===== ItemCF Similarities Output =====")
print("rows:", top_similarities_with_id.count())
top_similarities_with_id.orderBy("meal_index", "rank_no").show(30, truncate=False)

print("===== ItemCF Recommendations Output =====")
print("rows:", top_recommendations.count())
print("users:", top_recommendations.select("user_index").distinct().count())
top_recommendations.orderBy("user_index", "rank_no").show(30, truncate=False)

print("===== ItemCF Metrics =====")
metrics.show(truncate=False)

top_similarities.unpersist()
ratings.unpersist()
train_data.unpersist()
test_data.unpersist()

spark.stop()
