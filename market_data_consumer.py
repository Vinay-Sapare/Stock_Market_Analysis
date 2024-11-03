
import fastavro
import os
import json
from azure.storage.blob import BlobServiceClient

from confluent_kafka import DeserializingConsumer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer
from confluent_kafka.serialization import StringDeserializer

kafka_config = {
    "bootstrap.servers": "bootstrap_server_data",
    "sasl.mechanisms": "PLAIN",
    "security.protocol": "SASL_SSL",
    "sasl.username": "API_KEY",
    "sasl.password": "API_SECRET",
    "group.id": "group1",
    "auto.offset.reset": "earliest"
}

schema_registry_client = SchemaRegistryClient({
    "url": "ENDPOINT",
    "basic.auth.user.info": "{}:{}".format("SCHEMA_REGISTRY_API_KEY", "SCHEMA_REGISTRY_API_SECRET")
})

subject_name = "market_data-value"
schema_str = schema_registry_client.get_latest_version(subject_name).schema.schema_str

key_deserializer = StringDeserializer("utf_8")
avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

consumer = DeserializingConsumer({
    "bootstrap.servers": kafka_config["bootstrap.servers"],
    "security.protocol": kafka_config["security.protocol"],
    "sasl.mechanisms": kafka_config["sasl.mechanisms"],
    "sasl.username": kafka_config["sasl.username"],
    "sasl.password": kafka_config["sasl.password"],
    "group.id": kafka_config["group.id"],
    "auto.offset.reset": kafka_config["auto.offset.reset"],
    "enable.auto.commit": True,
    "auto.commit.interval.ms": 5000
})

consumer.subscribe(["market_data"])

# blob storage configuration

connection_string="AZURE BLOB STORAGE CONNECTION STRING"
container_name="CONTAINER_NAME"
blob_prefix="kafka-messages"

blob_service_client = BlobServiceClient.from_connection_string(connection_string)
blob_client = blob_service_client.get_blob_client(container=container_name, blob='storage_account_name')


try:
    while True:
        msg = consumer.poll(1.0)

        if msg is None:
            continue
        
        if msg.error():
            print(f"Consumer error: {msg.error()}")
            continue

        # Deserialize the message value using Avro
        value = avro_deserializer(msg.value(), None)

        blob_name=f"{blob_prefix}{msg.offset()}.json"

        blob_client=blob_service_client.get_blob_client(container=container_name,blob=blob_name)
        blob_client.upload_blob(json.dumps(value),overwrite=True)

        print(f"message has successfully sent to blob storage")




except KeyboardInterrupt:
    print("Stopping the consumer")

finally:
    consumer.close()
