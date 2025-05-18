from kafka import KafkaProducer
import json, time, random

producer = KafkaProducer(bootstrap_servers='localhost:9092',
                         value_serializer=lambda v: json.dumps(v).encode('utf-8'))

while True:
    data = {"gudang_id": random.choice(["G1", "G2", "G3"]),
            "kelembaban": random.randint(60, 80)}
    producer.send("sensor-kelembaban-gudang", value=data)
    print("Sent:", data)
    time.sleep(1)
