from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from delta.tables import DeltaTable
from pyspark.sql.types import ArrayType, MapType, StringType

# Initialize a Spark session
spark = SparkSession.builder.appName("DeltaTableCreation").getOrCreate()

# Read customer data from your source (e.g., another Delta table, Parquet files, etc.)
# Replace "your_source_path" with the actual path to your source data
customer_data = spark.read.format("delta").load("your_source_path")

# Extract unique customer IDs
unique_customer_ids = customer_data.select("customerid").distinct()

# Define an empty list of dictionaries as the initial value for the "stats" column
initial_stats_value = [{}]

# Define the schema for the "stats" column (list of dictionaries)
stats_schema = ArrayType(MapType(StringType(), StringType()))

# Create a DataFrame with the desired columns
delta_data = (
    unique_customer_ids
    .withColumn("status", lit("no processed"))
    .withColumn("timestamp", lit(None).cast("timestamp"))
    .withColumn("stats", lit(initial_stats_value).cast(stats_schema))
)

# Write the DataFrame as a Delta table to your S3 bucket
# Replace "your_s3_bucket_path" with the actual S3 bucket path where you want to store the Delta table
delta_data.write.format("delta").mode("overwrite").save("s3a://your_s3_bucket_path/delta_table")

# Once you have created the Delta table, you can use DeltaTable for future operations
delta_table = DeltaTable.forPath(spark, "s3a://your_s3_bucket_path/delta_table")

# Example: Update status and stats for a specific customer_id
customer_id_to_update = "customer1"
delta_table.update(
    condition=f"customerid = '{customer_id_to_update}'",
    set={"status": lit("processed"), "timestamp": current_timestamp(), "stats": lit([{"key": "value"}]).cast(stats_schema)}
)
