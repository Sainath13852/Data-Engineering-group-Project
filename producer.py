from kafka import KafkaProducer
import json
import time

producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

with open('tweets.json', 'r', encoding='utf-8') as file:
    tweets = json.load(file)

for tweet in tweets:
    producer.send('twitter-stream', tweet)
    print("Sent:", tweet)
    time.sleep(2)

producer.flush()
