from kafka import KafkaConsumer
import json
import mysql.connector

# MySQL Connection
conn = mysql.connector.connect(
    host="localhost",
    user="root",
    password="Apple@123",
    database="twitter_sentiment"
)

cursor = conn.cursor()

# Kafka Consumer
consumer = KafkaConsumer(
    'twitter-stream',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Waiting for messages...")

for message in consumer:

    data = message.value

    # ------------------
    # Cleaning Section
    # ------------------

    tweet_id = data.get("tweet_id")

    entity = str(data.get("entity", "")).strip()

    sentiment = str(data.get("sentiment", "")).strip().title()

    tweet = str(data.get("tweet", "")).strip()

    # Skip bad records
    if not tweet_id:
        continue

    if tweet == "":
        continue

    # ------------------
    # Store in MySQL
    # ------------------

    try:

        cursor.execute(
            """
            INSERT INTO tweets
            (tweet_id, entity, sentiment, tweet)
            VALUES (%s, %s, %s, %s)
            """,
            (
                tweet_id,
                entity,
                sentiment,
                tweet
            )
        )

        conn.commit()

        print(f"Inserted Tweet ID: {tweet_id}")

    except mysql.connector.Error:

        # Duplicate tweet_id ignored
        print(f"Duplicate Skipped: {tweet_id}")