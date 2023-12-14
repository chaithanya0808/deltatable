
Certainly! To find the history of address changes for each customer in PySpark using the customer_address dataframe 
with address_start_date and address_end_date, and create an array for each customer, you can use the following code:

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize a Spark session
spark = SparkSession.builder.appName("AddressChangeHistory").getOrCreate()

# Assume you have a dataframe named 'customer_address' with 'customer_id', 'address', 'address_start_date', 'address_end_date'
# Replace these column names with your actual column names

# Sample data creation for demonstration (replace with your actual data)
data = [
    (1, '123 Main St', '2022-01-01', '2022-01-15'),
    (1, '456 Oak St', '2022-01-16', '2022-02-28'),
    (1, '789 Elm St', '2022-03-01', '2022-03-31'),
    (2, '321 Pine St', '2022-01-01', '2022-02-28'),
    (2, '654 Birch St', '2022-03-01', '2022-03-31'),
]

columns = ['customer_id', 'address', 'address_start_date', 'address_end_date']
customer_address = spark.createDataFrame(data, columns)

# Window specification to order changes by timestamp
window_spec = Window.partitionBy("customer_id").orderBy("address_start_date")

# Create a column with the next address_start_date using lead function
customer_address = customer_address.withColumn(
    "next_address_start_date",
    F.lead("address_start_date").over(window_spec)
)

# Filter the rows where the next address_start_date is not null or the last row for each customer_id
filtered_df = customer_address.filter(
    (F.col("next_address_start_date").isNotNull()) |
    (F.row_number().over(window_spec.desc()) == 1)
)

# Collect the history of address changes into an array
address_history_df = filtered_df.groupBy("customer_id").agg(
    F.collect_list(
        F.struct(
            F.col("address"),
            F.col("address_start_date"),
            F.col("address_end_date").alias("change_end_date")
        )
    ).alias("address_history")
)

# Show the resulting dataframe
address_history_df.show(truncate=False)


We use the lead function to get the next address_start_date for each row.
Rows with the last address_start_date for each customer_id or those with a non-null next address_start_date are selected.
We then group by customer_id and collect the address history into an array using collect_list.
Replace the sample data creation part with your actual dataframe or adjust the code based on your specific dataframe structure and requirements.




  from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize a Spark session
spark = SparkSession.builder.appName("AddressChangeHistory").getOrCreate()

# Assume you have a dataframe named 'customer_address' with 'customer_id', 'address', 'address_start_date', 'address_end_date'
# Replace these column names with your actual column names

# Sample data creation for demonstration (replace with your actual data)
data = [
    (1, '123 Main St', '2022-01-01', '2022-01-15'),
    (1, '456 Oak St', '2022-01-16', '2022-02-28'),
    (1, '789 Elm St', '2022-03-01', '2059-12-31'),  # Active record with '31-12-2059' as end date
    (2, '321 Pine St', '2022-01-01', '2022-02-28'),
    (2, '654 Birch St', '2022-03-01', '2059-12-31'),  # Active record with '31-12-2059' as end date
]

columns = ['customer_id', 'address', 'address_start_date', 'address_end_date']
customer_address = spark.createDataFrame(data, columns)

# Replace '2059-12-31' with the desired end date for the active record
active_record_end_date = '2059-12-31'

# Window specification to order changes by timestamp
window_spec = Window.partitionBy("customer_id").orderBy("address_start_date")

# Create a column with the next address_start_date using lead function
customer_address = customer_address.withColumn(
    "next_address_start_date",
    F.lead("address_start_date").over(window_spec)
)

# If the next_address_start_date is null, replace it with the active_record_end_date
customer_address = customer_address.withColumn(
    "next_address_start_date",
    F.when(F.col("next_address_start_date").isNull(), active_record_end_date)
    .otherwise(F.col("next_address_start_date"))
)

# Filter the rows where the next address_start_date is not equal to the current address_end_date
filtered_df = customer_address.filter(
    F.col("next_address_start_date") != F.col("address_end_date")
)

# Collect the history of address changes into an array
address_history_df = filtered_df.groupBy("customer_id").agg(
    F.collect_list(
        F.struct(
            F.col("address"),
            F.col("address_start_date"),
            F.col("address_end_date").alias("change_end_date")
        )
    ).alias("address_history")
)

# Show the resulting dataframe
address_history_df.show(truncate=False)

