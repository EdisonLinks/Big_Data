from pyspark.sql import SparkSession

spark = (
    SparkSession.builder
    .appName("SparkSQLAnalysisFromHive")
    .enableHiveSupport()
    .getOrCreate()
)

df = spark.table("menu_recommendation.dwd_user_meal_rating_clean")
df.createOrReplaceTempView("ratings")

summary = spark.sql("""
SELECT
    COUNT(*) AS rating_count,
    COUNT(DISTINCT user_id) AS user_count,
    COUNT(DISTINCT meal_id) AS meal_count,
    AVG(rating) AS avg_rating,
    percentile_approx(rating, 0.5) AS median_rating,
    STDDEV(rating) AS std_rating
FROM ratings
""")

rating_distribution = spark.sql("""
SELECT
    rating,
    COUNT(*) AS rating_count
FROM ratings
GROUP BY rating
ORDER BY rating
""")

popular_meals = spark.sql("""
SELECT
    meal_id,
    COUNT(*) AS rating_count,
    AVG(rating) AS avg_rating
FROM ratings
GROUP BY meal_id
ORDER BY rating_count DESC, avg_rating DESC
LIMIT 10
""")

active_users = spark.sql("""
SELECT
    user_id,
    COUNT(*) AS rating_count,
    AVG(rating) AS avg_rating
FROM ratings
GROUP BY user_id
ORDER BY rating_count DESC, avg_rating DESC
LIMIT 10
""")

summary.show(truncate=False)
rating_distribution.show(truncate=False)
popular_meals.show(truncate=False)
active_users.show(truncate=False)

summary.write.mode("overwrite").option("header", "true").csv("hdfs:///Data/output/sql_analysis/summary")
rating_distribution.write.mode("overwrite").option("header", "true").csv("hdfs:///Data/output/sql_analysis/rating_distribution")
popular_meals.write.mode("overwrite").option("header", "true").csv("hdfs:///Data/output/sql_analysis/popular_meals")
active_users.write.mode("overwrite").option("header", "true").csv("hdfs:///Data/output/sql_analysis/active_users")

spark.stop()
