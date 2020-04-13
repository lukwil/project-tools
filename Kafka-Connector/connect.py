import kafka
import json

from settings import *

kafka_consumer = kafka.KafkaConsumer(KAFKA_TOPIC,
                                     client_id=KAFKA_CLIENT,
                                     bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                                     security_protocol='SASL_PLAINTEXT',
                                     sasl_mechanism='PLAIN',
                                     sasl_plain_username=KAFKA_USERNAME,
                                     sasl_plain_password=KAFKA_PASSWORD,
                                     api_version=(0, 10, 1),
                                     value_deserializer=lambda m: json.loads(m.decode('utf-8')))

for message in kafka_consumer:
    print(message)


kafka_producer = kafka.KafkaProducer(client_id=KAFKA_CLIENT,
                                     bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                                     security_protocol="SASL_PLAINTEXT",
                                     sasl_mechanism='PLAIN',
                                     sasl_plain_username=KAFKA_USERNAME,
                                     sasl_plain_password=KAFKA_PASSWORD,
                                     api_version=(0, 10, 1),
                                     value_serializer=lambda m: json.dumps(m).encode('utf-8'))

kafka_producer.send(KAFKA_TOPIC, {'key': 'value'})
