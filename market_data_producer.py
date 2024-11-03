import pandas as pd
import time
from decimal import *
import threading
from time import sleep
from uuid import uuid4,UUID

from confluent_kafka import SerializingProducer # The SerializingProducer is used to convert an object into a byte stream and produce the message in a Kafka topic.
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka.serialization import StringSerializer

kafka_config={
    "bootstrap.servers":"bootstrap_server_data",
    "sasl.mechanisms":"PLAIN",
    "security.protocol":"SASL_SSL",
    "sasl.username":"API_KEY",
    "sasl.password":"API_SECRET"
}

schema_registry_client=SchemaRegistryClient({
    "url":"ENDPOINT",
    "basic.auth.user.info":"{}:{}".format("SCHEMA_REGISTRY_API_KEY","SCHEMA_REGISTRY_API_SECRET")
})

def delivery_report(error,message):

    if error is not None:
        print(f"delivery failed at {message.key()} and error is {error}")
        return 
    
    print(f"user_record is sucessfully produced at {message.partition()} key is [{message.key()}] at an offset of {message.offset()}")

subject_name="market_data-value" # subject name is schema registry value
schema_str=schema_registry_client.get_latest_version(subject_name).schema.schema_str

key_serializer=StringSerializer("utf_8")
avro_serializer=AvroSerializer(schema_registry_client,schema_str)

producer=SerializingProducer({
    "bootstrap.servers":kafka_config["bootstrap.servers"],
    "sasl.mechanisms":kafka_config["sasl.mechanisms"],
    "security.protocol":kafka_config["security.protocol"],
    "sasl.username":kafka_config["sasl.username"],
    "sasl.password":kafka_config["sasl.password"],
    "key.serializer":key_serializer,
    "value.serializer":avro_serializer
})

data=pd.read_csv(r"C:\Users\Vinay Sapare\Downloads\indexProcessed.csv")

for index,row in data.iterrows():

    value=row.to_dict()
    producer.produce(topic="market_data",key=str(index),value=value,on_delivery=delivery_report)

    producer.flush()

    time.sleep(2) # sleep - it will produce a message in the kafka topic every 2 seconds