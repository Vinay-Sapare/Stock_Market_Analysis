# Stock Market Analysis Using Kafka

## Introduction 
This project focuses on real-time stock market analysis using Apache Kafka for data streaming, Azure Blob Storage for scalable storage, and Snowflake for advanced analytics. 
By integrating these technologies, we create a seamless pipeline that captures and analyzes stock market data, enabling timely insights and improved trading strategies.

## Services used
- **Confluent Kafka -** It is a commercial version of Apache Kafka that adds extra tools and features for easier management and integration.
It includes a monitoring dashboard, schema management,easy connectors to other data sources, and SQL-like querying for real-time data processing.
This makes it better suited for larger, enterprise applications.

- **Azure Blob Storage -** It is a cloud service for storing large amounts of unstructured data like text, images, and videos. 
It offers scalable storage with different tiers (hot, cool, archive) to optimize costs based on access frequency. 
It's ideal for data lakes, backups, and big data analytics.

- **Snowflake -** It is a cloud-based data warehousing platform that lets organizations store and analyze data efficiently.
It separates storage from computing, allowing for flexible scaling
and quick access to data. Snowflake supports various data formats and offers features like data sharing and SQL querying, making it ideal for large-scale analytics.

## Execution Flow

Load CSV file --> Read CSV file and Send to kafka topic --> Read Data from kafka topic --> Store data in Azure Blob Storage --> Continuous Load Data into Snowflake using snowpipe
--> Run queries on snowflake 

## Architecture 

## Producer Code
```
import pandas as pd
import time
from decimal import *
import threading
from time import sleep
from uuid import uuid4,UUID

from confluent_kafka import SerializingProducer
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

subject_name="market_data-value" 
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

data=pd.read_csv(r"csv_file_path")

for index,row in data.iterrows():

    value=row.to_dict()
    producer.produce(topic="topic_name",key=str(index),value=value,on_delivery=delivery_report)

    producer.flush()

    time.sleep(2)
```

## Consumer Code
```
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
```

## Load Data into Snowflake Datawarehouse

```
CREATE TABLE stock_market_data (
    Index VARCHAR(256),
    Date DATE,
    Open float,
    High float,
    Low float,
    Close float,
    Adj_Close float,
    Volume float,
    CloseUSD float
    
);

CREATE FILE FORMAT json_format
    TYPE="json"
    TIME_FORMAT=auto;

# Notification integration is refers to the capability to receive event notifications from external storage services
(like AWS S3, Azure Blob Storage, or Google Cloud Storage) to trigger actions in Snowflake, particularly for data loading processes.

CREATE NOTIFICATION INTEGRATION AZURE_NOTIFY_INT
TYPE=QUEUE
NOTIFICATION_PROVIDER=AZURE_STORAGE_QUEUE
ENABLED=TRUE
AZURE_STORAGE_QUEUE_PRIMARY_URI="AZURE_STORAGE_QUEUE_URI"
AZURE_TENANT_ID="AZURE_TENANT_ID";

# creating a stage - Stages provide a convenient way to load data from external sources into Snowflake tables.You can copy data from files stored in stages using the COPY INTO command.

CREATE STAGE market_stage
URL="AZURE://<storage_account_name>.blob.core.windows.net/<container_name>"
CREDENTIALS=(AZURE_SAS_TOKEN='AZURE_SAS_TOKEN')

# Creating a Snowpipe for continuous loading of data when a file arrives in Blob Storage

CREATE PIPE azure_pipe
    AUTO_INGEST=True
    INTEGRATION="AZURE_NOTIFY_INT"
    AS COPY INTO market_data
    FROM @market_stage
    FILE_FORMAT=json_format
    MATCH_BY_COLUMN_NAME=case_insensitive;

```

