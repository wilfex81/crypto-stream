import json

from kafka import KafkaConsumer


consumer = KafkaConsumer(
    "Hello-Topic",
    bootstrap_servers="localhost:9092",
    auto_offset_reset="earliest",
    enable_auto_commit=True,
    group_id="toy-consumer-group",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),

)


if __name__ == "__main__":
    print("Listening on Hello-Topics...")
    for message in consumer:
        print(
            f"partition={message.partition} offset={message.offset} "
            f"value={message.value}"
        )