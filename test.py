import pandas as pd
import warnings
from kafka import KafkaProducer
import time

warnings.simplefilter(action='ignore', category=FutureWarning)
def read_csv_file(csv_file):
    try:
        # Read CSV file using pandas
        df = pd.read_csv(csv_file)
        return df
    except FileNotFoundError:
        print("File not found. Please provide the correct path to the CSV file.")
        return None
    except Exception as e:
        print("An error occurred:", e)
        return None

def publish_to_kafka(df, topic):
    try:
        producer = KafkaProducer(bootstrap_servers="localhost:9092")

        # Convert DataFrame rows to JSON and publish to Kafka topic
        for _, row in df.iterrows():
            message = row.to_json().encode("utf-8")
            producer.send(topic, value=message)

            producer.flush()
            print("Messages published to Kafka topic:", topic)
            time.sleep(5)


    except Exception as e:
        print("An error occured while publishing to Kafka", e)


def main():
    # Path to the CSV file
    csv_file = "/media/elmehdi/SQUAD/Processed_UNSW-NB15.csv"  # Update with the correct path to your CSV file

    # Kafka topic to publish messages to
    kafka_topic = "market"
    # Read CSV file
    df = read_csv_file(csv_file)
    if df is not None:
        print("Contents of the CSV file:")
        print(df)

        # Publish CSV data to Kafka
        publish_to_kafka(df, kafka_topic)

if __name__ == "__main__":
    main()
