import sys
from awsglue.transforms import *
from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from pyspark.sql.functions import col, to_date, upper, coalesce, lit
from awsglue.dynamicframe import DynamicFrame

## Initialize contexts
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)

# --- Define S3 Paths with your bucket names ---
s3_input_path = "s3://product-reviews-landing-bucket/"
s3_processed_path = "s3://product-reviews-processed-bucket/processed-data/"
s3_analytics_path = "s3://product-reviews-processed-bucket/Athena Results/"

# --- Read the data from the S3 landing zone ---
dynamic_frame = glueContext.create_dynamic_frame.from_options(
    connection_type="s3",
    connection_options={"paths": [s3_input_path], "recurse": True},
    format="csv",
    format_options={"withHeader": True, "inferSchema": True},
)

# Convert to a standard Spark DataFrame for easier transformation
df = dynamic_frame.toDF()

# --- Perform Transformations ---
# 1. Cast 'rating' to integer and fill null values with 0
df_transformed = df.withColumn("rating", coalesce(col("rating").cast("integer"), lit(0)))

# 2. Convert 'review_date' string to a proper date type
df_transformed = df_transformed.withColumn("review_date", to_date(col("review_date"), "yyyy-MM-dd"))

# 3. Fill null review_text with a default string
df_transformed = df_transformed.withColumn(
    "review_text",
    coalesce(col("review_text"), lit("No review text"))
)

# 4. Convert product_id to uppercase for consistency
df_transformed = df_transformed.withColumn("product_id_upper", upper(col("product_id")))

# --- Write the full transformed data to S3 (Good practice) ---
# This saves the clean, complete dataset to the 'processed-data' folder
glue_processed_frame = DynamicFrame.fromDF(df_transformed, glueContext, "transformed_df")
glueContext.write_dynamic_frame.from_options(
    frame=glue_processed_frame,
    connection_type="s3",
    connection_options={"path": s3_processed_path},
    format="csv"
)

# --- Run Spark SQL Queries within the Job ---

# 1. Create a temporary view in Spark's memory
df_transformed.createOrReplaceTempView("product_reviews")

# 2. Analytics Query 1: Average rating per product
df_analytics_result = spark.sql("""
    SELECT 
        product_id_upper, 
        AVG(rating) AS average_rating,
        COUNT(*) AS review_count
    FROM product_reviews
    GROUP BY product_id_upper
    ORDER BY average_rating DESC
""")

# Write Query 1 results
print(f"Writing analytics results to {s3_analytics_path}...")
analytics_result_frame = DynamicFrame.fromDF(
    df_analytics_result.repartition(1),
    glueContext,
    "analytics_df"
)
glueContext.write_dynamic_frame.from_options(
    frame=analytics_result_frame,
    connection_type="s3",
    connection_options={"path": s3_analytics_path},
    format="csv"
)

# ---------------------------------------------------------
# Additional Required Queries
# ---------------------------------------------------------

# 2. Date wise review count: total number of reviews submitted per day
df_daily_counts = spark.sql("""
    SELECT 
        review_date,
        COUNT(*) AS review_count
    FROM product_reviews
    GROUP BY review_date
    ORDER BY review_date
""")

daily_counts_frame = DynamicFrame.fromDF(
    df_daily_counts.repartition(1),
    glueContext,
    "daily_counts_df"
)
glueContext.write_dynamic_frame.from_options(
    frame=daily_counts_frame,
    connection_type="s3",
    connection_options={"path": s3_analytics_path + "daily_review_counts/"},
    format="csv"
)

# 3. Top 5 Most Active Customers: customers with the most reviews
df_top_customers = spark.sql("""
    SELECT 
        customer_id,
        COUNT(*) AS review_count
    FROM product_reviews
    GROUP BY customer_id
    ORDER BY review_count DESC
    LIMIT 5
""")

top_customers_frame = DynamicFrame.fromDF(
    df_top_customers.repartition(1),
    glueContext,
    "top_customers_df"
)
glueContext.write_dynamic_frame.from_options(
    frame=top_customers_frame,
    connection_type="s3",
    connection_options={"path": s3_analytics_path + "top_5_customers/"},
    format="csv"
)

# 4. Overall Rating Distribution: count of each star rating
df_rating_distribution = spark.sql("""
    SELECT 
        rating,
        COUNT(*) AS rating_count
    FROM product_reviews
    GROUP BY rating
    ORDER BY rating
""")

rating_distribution_frame = DynamicFrame.fromDF(
    df_rating_distribution.repartition(1),
    glueContext,
    "rating_distribution_df"
)
glueContext.write_dynamic_frame.from_options(
    frame=rating_distribution_frame,
    connection_type="s3",
    connection_options={"path": s3_analytics_path + "rating_distribution/"},
    format="csv"
)

# ---------------------------------------------------------

job.commit()
