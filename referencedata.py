Certainly! If you want to create a new column in a PySpark DataFrame based on a reference dictionary for gender types, you can use the withColumn function along with the when function. Here's an example:

python
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when

# Create a Spark session
spark = SparkSession.builder.appName("GenderMapping").getOrCreate()

# Sample DataFrame
data = [("John", "M"), ("Jane", "F"), ("Alex", "O")]
columns = ["name", "gender"]
df = spark.createDataFrame(data, columns)

# Reference gender dictionary
gender_dict = {"M": "Male", "F": "Female", "O": "Other"}

# Create a new column 'gender_desc' based on the dictionary mapping
df = df.withColumn("gender_desc", when(col("gender").isin(gender_dict.keys()), gender_dict[col("gender")]).otherwise("Unknown"))

# Show the resulting DataFrame
df.show()


This code adds a new column named 'gender_desc' to the DataFrame, which contains the corresponding gender descriptions based on the dictionary mapping. Adjust the sample DataFrame and gender dictionary with your actual data.
