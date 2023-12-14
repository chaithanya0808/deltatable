from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize a Spark session
spark = SparkSession.builder.appName("AddressHistory").getOrCreate()

# Assuming you have two dataframes named customer_df and customer_address_df
# Replace "party_number" and "primary_entity_number" with your actual column names

# Join the two dataframes on the common column
joined_df = customer_df.join(
    customer_address_df,
    (customer_df["party_number"] == customer_address_df["primary_entity_number"]) &
    (customer_df["last_updated"] >= customer_address_df["change_begin_date"]),
    how="inner"
)

# Define a window specification to order changes by timestamp
window_spec = Window.partitionBy("party_number").orderBy("change_begin_date")

# Create a column with the next change_begin_date using lead function
joined_df = joined_df.withColumn(
    "next_change_begin_date",
    F.lead("change_begin_date").over(window_spec)
)

# Filter the rows where the next change_begin_date is not null or the last row for each party_number
filtered_df = joined_df.filter(
    (F.col("next_change_begin_date").isNotNull()) |
    (F.row_number().over(window_spec.desc()) == 1)
)

# Collect the history of address changes into an array
address_history_df = filtered_df.groupBy("party_number").agg(
    F.collect_list(
        F.struct(
            F.col("address"),
            F.col("change_begin_date"),
            F.col("next_change_begin_date").alias("change_end_date")
        )
    ).alias("address_history")
)

# Show the resulting dataframe
address_history_df.show(truncate=False)
