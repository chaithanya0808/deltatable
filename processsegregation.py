from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp
from delta.tables import DeltaTable
from pyspark.sql.types import ArrayType, MapType, StringType

# Initialize a Spark session
spark = SparkSession.builder.appName("CustomerProcessing").getOrCreate()

# Load the Delta table created earlier
delta_table_path = "s3a://your_s3_bucket_path/delta_table"
delta_table = DeltaTable.forPath(spark, delta_table_path)

# Example: Assume you have a DataFrame representing your customer changes
# Replace "your_customer_changes_path" with the actual path to your customer changes DataFrame
customer_changes_path = "s3a://your_s3_bucket_path/customer_changes"
customer_changes_df = spark.read.format("delta").load(customer_changes_path)

# Process each customer ID
for customer_row in delta_table.toDF().collect():
    customer_id = customer_row["customerid"]

    # Placeholder for your custom processing logic
    # Replace the following with your actual processing logic
    try:
        # Your processing logic here...

        # Example: Assume you are processing the customer_changes_df DataFrame
        customer_changes_processed_df = customer_changes_df.filter(
            customer_changes_df["customer_id"] == customer_id
        )

        # Get the count of processed phone and email records
        phone_records_processed = customer_changes_processed_df.filter(
            customer_changes_processed_df["change_type"] == "phone"
        ).count()

        email_records_processed = customer_changes_processed_df.filter(
            customer_changes_processed_df["change_type"] == "email"
        ).count()

        # Example: Mark the customer as processed and update stats
        processed_stats = {
            "phone_records_processed": phone_records_processed,
            "email_records_processed": email_records_processed,
        }
        delta_table.update(
            condition=f"customerid = '{customer_id}'",
            set={
                "status": lit("processed"),
                "timestamp": current_timestamp(),
                "stats": lit([processed_stats]).cast(stats_schema),
            }
        )

    except Exception as e:
        # If processing fails, mark the customer as failed
        delta_table.update(
            condition=f"customerid = '{customer_id}'",
            set={"status": lit("failed"), "timestamp": current_timestamp(), "stats": lit(None).cast(stats_schema)}
        )
