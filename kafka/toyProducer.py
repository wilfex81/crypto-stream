"""
Simple test messages to confirm Kafka worsk
"""


import json
import time

from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v:json.dumps(v).encode("utf-8"),
)

if __name__ == "__main__":
    for i in range(20):
        message = {"seq": i, "text": f"hello kafka {i}"}
        producer.send("Hello-Topic", value=message)
        print(f"Sent: {message}")
        time.sleep(1)

    producer.flush()