import base64
import boto3
import json
from time import sleep
from json import dumps
from kafka import KafkaProducer
from kafka.errors import KafkaError

reply_topic_name = 'reply_topic'

def lambda_handler(event, context):
    # Initialize the producer inside the handler
    producer = KafkaProducer(
        bootstrap_servers=[
            'b-2.publicmsktest.6c22nc.c6.kafka.eu-central-1.amazonaws.com:9092',
            'b-1.publicmsktest.6c22nc.c6.kafka.eu-central-1.amazonaws.com:9092'
        ],
        value_serializer=lambda x: dumps(x).encode('utf-8'),
        key_serializer=lambda x: dumps(x).encode('utf-8')
    )

    try:
        print(event)
        for partition_key in event['records']:
            partition_value = event['records'][partition_key]
            for record_value in partition_value:
                decoded_value = (base64.b64decode(record_value['value'])).decode()
                decoded_key = (base64.b64decode(record_value['key'])).decode()
                print(decoded_value)
                try:
                    producer.send(reply_topic_name, value=decoded_value, key=decoded_key)
                    #producer.send(reply_topic_name, value=decoded_value)
                except KafkaError as e:
                    print(f"Error sending message: {e}")
                    raise e
            print("Reply sent")
            producer.flush()
    finally:
        producer.close()
    
    return {
        'statusCode': 200,
        'body': 'Messages sent to Kafka'
    }
