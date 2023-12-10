from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from delta.tables import DeltaTable
from pyspark.sql.types import ArrayType, MapType, StringType

# Initialize a Spark session
spark = SparkSession.builder.appName("DeltaTableProcessing").getOrCreate()

# Load the Delta table created earlier
delta_table = DeltaTable.forPath(spark, "s3a://your_s3_bucket_path/delta_table")

# Process each customer ID
for customer_row in delta_table.toDF().collect():
    customer_id = customer_row["customerid"]

    # Placeholder for your custom processing logic
    # Replace the following with your actual processing logic
    try:
        # Your processing logic here...

        # Example: Mark the customer as processed and update stats
        processed_stats = {"table_name": "your_table_name", "rows_processed": 10}  # Replace with actual stats
        delta_table.update(
            condition=f"customerid = '{customer_id}'",
            set={"status": lit("processed"), "timestamp": current_timestamp(), "stats": lit([processed_stats]).cast(stats_schema)}
        )

    except Exception as e:
        # If processing fails, mark the customer as failed
        delta_table.update(
            condition=f"customerid = '{customer_id}'",
            set={"status": lit("failed"), "timestamp": current_timestamp(), "stats": lit(None).cast(stats_schema)}
        )
