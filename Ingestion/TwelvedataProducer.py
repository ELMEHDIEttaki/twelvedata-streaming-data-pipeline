from twelvedata import TDClient
import pandas as pd
import websocket
import time
from kafka import KafkaProducer
import warnings
import json


warnings.simplefilter(action='ignore', category=FutureWarning)
historical_messages = []

def on_event(e):
    print(e)
    historical_messages.append(e)


producer = KafkaProducer(bootstrap_servers="localhost:9092")
kafka_topic = "market"

API_KEY = ""
td = TDClient(API_KEY)
ws = td.websocket(symbols="BTC/USD", on_event=on_event)
ws.subscribe(['ETH/BTC', 'USD', 'AAPL'])
ws.connect()


try:
    while True:
        print("messages recieved: ", len(historical_messages))
        ws.heartbeat()
        for message in historical_messages:
            serialized_message = json.dumps(message).encode('utf-8')
            #print(serialized_message)
            #Output : b'{"event": "heartbeat", "status": "ok"}'
            producer.send(kafka_topic, value=serialized_message)
        producer.flush()
        print("Messages published to Kafka topic:", kafka_topic)
        time.sleep(10)

except KeyboardInterrupt:
    print("Interrupted by me :) !")
