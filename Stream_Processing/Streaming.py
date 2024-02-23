from pyspark.sql import SparkSession
from pyspark.sql.functions import *




if __name__ == "__main__":
    #Create SparkSession

    spark = SparkSession.builder \
            .appName("KafkaStreamingApp") \
            .getOrCreate()
    
    # Define Kafka parameters

    kafka_params = {
        "kafka.bootstrap.servers": "localhost:9092",
        "subscribe": "market"
        
    }

    # Define the streaming DataFRame that represents data from Kafka
    df = spark \
        .readStream \
        .format("kafka") \
        .options(**kafka_params) \
        .load()
    
    processed_df = df.selectExpr("CAST(value AS STRING)")

    query = processed_df \
        .writeStream \
        .outputMode("append") \
        .format("console") \
        .trigger(processingTime="10 seconds")  # Adjust trigger interval as needed

    query.start().awaitTermination()
