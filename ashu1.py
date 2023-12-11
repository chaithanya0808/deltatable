from pyspark.sql import SparkSession
from pyspark.sql import Window
from pyspark.sql.functions import col, lag

# Create a Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Assuming df_history is your history DataFrame
df_history = ...

# Assuming 'customer_id' is the customer identifier column
customer_id_to_check = "your_customer_id"

# Define columns to compare
columns_to_compare = ['col1', 'col2', 'col3', 'col4', 'col5', 'col6', 'col7', 'col8', 'col9', 'col10']

# Define a window specification based on 'customer_id' and order by a timestamp column
window_spec = Window.partitionBy('customer_id').orderBy('timestamp_column')

# Create new columns for each column in columns_to_compare, representing the previous value
for column in columns_to_compare:
    df_history = df_history.withColumn(f'{column}_prev', lag(col(column)).over(window_spec))

# Filter rows where any of the current and previous values are different
changed_records = df_history.filter(
    (col('customer_id') == customer_id_to_check) &
    (
        (col('col1') != col('col1_prev')) |
        (col('col2') != col('col2_prev')) |
        (col('col3') != col('col3_prev')) |
        (col('col4') != col('col4_prev')) |
        (col('col5') != col('col5_prev')) |
        (col('col6') != col('col6_prev')) |
        (col('col7') != col('col7_prev')) |
        (col('col8') != col('col8_prev')) |
        (col('col9') != col('col9_prev')) |
        (col('col10') != col('col10_prev'))
    )
)

# Display the changed records
changed_records.show()


v2:

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sha2, concat_ws

# Create a Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Assuming df_history is your history DataFrame
df_history = ...

# Assuming 'customer_id' is the customer identifier column
customer_id_to_check = "your_customer_id"

# Define columns to compare
columns_to_compare = ['col1', 'col2', 'col3', 'col4', 'col5', 'col6', 'col7', 'col8', 'col9', 'col10']

# Concatenate selected columns into a single string and hash it
df_history = df_history.withColumn(
    'row_hash',
    sha2(concat_ws("|", *[col(column) for column in columns_to_compare]), 256)
)

# Identify changed records for the specific customer
changed_records = df_history.filter(
    (col('customer_id') == customer_id_to_check)
)

# Display the changed records
changed_records.show()


v3:
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create a Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Assuming df is your DataFrame
df = ...

# Assuming 'customer_id' is the customer identifier column
customer_id_to_check = "your_customer_id"

# Define columns to compare
columns_to_compare = ['col1', 'col2', 'col3', 'col4', 'col5', 'col6', 'col7', 'col8', 'col9', 'col10']

# Filter DataFrame based on the customer ID and select only the columns of interest
df_filtered = df.filter(col('customer_id') == customer_id_to_check).select(*columns_to_compare)

# Apply conditions for each column to check for changes
conditions = [(col(f'{col_name}') != col(f'{col_name}_prev')) for col_name in columns_to_compare]

# Create new columns for each column in columns_to_compare, representing the previous value
for column in columns_to_compare:
    df = df.withColumn(f'{column}_prev', col(column).shift(1).over(Window().orderBy('timestamp_column')))

# Apply conditions using the filter method
changed_records = df_filtered.filter(*conditions)

# Display the changed records
changed_records.show()

v4:
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, concat_ws

# Create a Spark session
spark = SparkSession.builder.appName("example").getOrCreate()

# Assuming df is your DataFrame
df = ...

# Assuming 'customer_id' is the customer identifier column
customer_id_to_check = "your_customer_id"

# Define columns to compare
columns_to_compare = ['col1', 'col2', 'col3', 'col4', 'col5', 'col6', 'col7', 'col8', 'col9', 'col10']

# Filter DataFrame based on the customer ID and select only the columns of interest
df_filtered = df.filter(col('customer_id') == customer_id_to_check).select(*columns_to_compare)

# Create a composite key by concatenating selected columns
composite_key_expr = concat_ws("|", *[col(column) for column in columns_to_compare])
df = df.withColumn("composite_key", composite_key_expr)

# Identify changed records using distinct and count
changed_records = df.groupBy("composite_key").agg(count("*").alias("count")).filter(col("count") > 1)

# Display the changed records
changed_records.show(truncate=False)
