from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize a Spark session
spark = SparkSession.builder.appName("AddressChangeAnalysis").getOrCreate()

# Assume you have a Delta table named 'delta_unique_customers'
delta_table_path = "s3a://your_s3_bucket_path/delta_unique_customers"

# Read the Delta table with unique customer IDs
delta_customers_df = spark.read.format("delta").load(delta_table_path)

# Assume you have the result dataframe named 'address_history_df' from the previous code snippet
# If not, replace it with the appropriate dataframe containing address change history

# Join the Delta table with the address history dataframe on the customer ID
joined_df = delta_customers_df.join(
    address_history_df,
    delta_customers_df["customer_id"] == address_history_df["party_number"],
    how="inner"
)

# Count the number of address changes for each customer
address_change_count_df = joined_df.groupBy("customer_id").agg(
    F.countDistinct("address_history.address").alias("address_change_count")
)

# Show the resulting dataframe
address_change_count_df.show(truncate=False)
