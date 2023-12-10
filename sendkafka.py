from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_json
from confluent_kafka import Producer
import io
import avro.schema
from avro.io import AvroWriter, BinaryEncoder

# Initialize a Spark session
spark = SparkSession.builder.appName("CustomerProcessing").getOrCreate()

# Example: Read your main customer table
# Replace "your_main_customer_table_path" with the actual path to your main customer table
main_customer_table_path = "s3a://your_s3_bucket_path/main_customer_table"
main_customer_df = spark.read.format("delta").load(main_customer_table_path)

# Define an Avro schema
avro_schema = avro.schema.Parse(open("your_avro_schema.avsc", "r").read())  # Replace with your Avro schema file

# Define Kafka producer configuration
kafka_broker = "your_kafka_broker"  # Replace with your Kafka broker information
kafka_topic = "your_kafka_topic"  # Replace with your Kafka topic

producer_config = {
    "bootstrap.servers": kafka_broker,
    # Include any other Kafka producer configuration parameters
}

# Create a Kafka producer
producer = Producer(producer_config)

# Serialize and send each customer record to Kafka
for customer_row in main_customer_df.collect():
    # Extract relevant information from the customer DataFrame
    customer_id = customer_row["customer_id"]
    # Include any other relevant columns

    # Serialize data using Avro schema
    record = {
        "customer_id": customer_id,
        # Include any other relevant fields based on your Avro schema
    }

    avro_bytes_writer = io.BytesIO()
    avro_writer = AvroWriter(avro_bytes_writer, avro_schema)
    avro_writer.write(record)
    avro_writer.close()

    avro_bytes = avro_bytes_writer.getvalue()

    # Produce Avro data to Kafka
    producer.produce(kafka_topic, value=avro_bytes)

# Flush the producer to ensure all messages are sent
producer.flush()
