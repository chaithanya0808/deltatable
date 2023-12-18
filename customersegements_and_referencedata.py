from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize a Spark session
spark = SparkSession.builder.appName("AddressChangeHistory").getOrCreate()

# Assume you have a dataframe named 'customer_address' with 'customer_id', 'address', 'address_start_date', 'address_end_date', 'address_type'
# Replace these column names with your actual column names

# Sample data creation for demonstration (replace with your actual data)
data = [
    (1, '123 Main St', '2022-01-01', '2022-01-15', 'organization'),
    (1, '456 Oak St', '2022-01-16', '2022-02-28', 'organization'),
    (1, '789 Elm St', '2022-03-01', '2059-12-31', 'individual'),  # Active record with '31-12-2059' as end date
    (2, '321 Pine St', '2022-01-01', '2022-02-28', 'individual'),
    (2, '654 Birch St', '2022-03-01', '2059-12-31', 'organization'),  # Active record with '31-12-2059' as end date
]

columns = ['customer_id', 'address', 'address_start_date', 'address_end_date', 'address_type']
customer_address = spark.createDataFrame(data, columns)

# Replace '2059-12-31' with the desired end date for the active record
active_record_end_date = '2059-12-31'

# Window specification to order changes by timestamp and partition by customer_id and address_type
window_spec = Window.partitionBy("customer_id", "address_type").orderBy("address_start_date")

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

# Add Latest_record_indicator column
customer_address = customer_address.withColumn(
    "Latest_record_indicator",
    F.when(
        (F.col("address_end_date") == active_record_end_date) &
        (F.col("next_address_start_date") == active_record_end_date),
        'Y'
    ).otherwise('N')
)

# Filter the rows where the next address_start_date is not equal to the current address_end_date and ignore records where address_start_date and next_address_start_date are the same
filtered_df = customer_address.filter(
    (F.col("next_address_start_date") != F.col("address_end_date")) &
    (F.col("address_start_date") != F.col("next_address_start_date"))
)

# Collect the history of address changes into an array
address_history_df = filtered_df.groupBy("customer_id", "address_type").agg(
    F.collect_list(
        F.struct(
            F.col("address"),
            F.col("address_start_date"),
            F.col("address_end_date").alias("change_end_date"),
            F.col("Latest_record_indicator")
        )
    ).alias("address_history")
)

# Show the resulting dataframe
address_history_df.show(truncate=False)



Read from csv

# Read reference data from CSV file into a DataFrame
reference_data_df = spark.read.csv("path/to/reference_data.csv", header=True, inferSchema=True)

# Join the actual DataFrame with the reference DataFrame on the 'id' column
joined_df = actual_data_df.join(reference_data_df, on='id', how='left')

# Create a new column 'full_name' by concatenating 'first_name' and 'last_name'
result_df = joined_df.withColumn('full_name', F.concat(F.col('first_name'), F.lit(' '), F.col('last_name')))

# Show the resulting DataFrame
result_df.show(truncate=False)


from pyspark.sql import functions as F

# Read gender reference data from CSV file into a DataFrame
gender_reference_df = spark.read.csv("path/to/gender_reference.csv", header=True, inferSchema=True)

# Use the reference data to map gender codes to meaningful names
result_df = actual_data_df.withColumn(
    'gender_full_name',
    F.when(F.col('gender') == 'M', F.lit('Male'))
    .when(F.col('gender') == 'F', F.lit('Female'))
    .otherwise(F.col('gender'))
)

# Show the resulting DataFrame
result_df.show(truncate=False)

