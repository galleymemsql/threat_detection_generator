import csv
import random
from datetime import datetime
from confluent_kafka import Producer
import threading
import time
import json
from collections import Counter
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns
from tensorflow import keras
from tensorflow.keras.models import Model
import tensorflow.keras.backend as K
from sklearn.metrics import accuracy_score, precision_score, recall_score
from sklearn.metrics import confusion_matrix
from tqdm import tqdm
import numpy as np
import os

# Environment Variables
kafka_broker = os.environ.get('kafka_broker')
kafka_username = os.environ.get('kafka_username')
kafka_password = os.environ.get('kafka_password')


def read_csv(file_path):
    return pd.read_csv(file_path)

def random_data_generator(df):
    counter = 0  # Initialize a counter variable
    while True:
        random_data = df.sample(n=1)
        
        # Run prediction using the model
        model_input = random_data.iloc[:, :-1]  # Assuming the last column is the label/target
        print(f"Model Input: {model_input}")
        
        model_res = intermediate_layer_model.predict(K.constant(model_input.values))  # Convert DataFrame to values
        # print(f"Model Output: {model_res}")
        
        # Normalize the model output vector
        # magnitude = np.linalg.norm(model_res)
        # normalized_model_res = model_res / magnitude
        # print(f"Normalized Model Output: {normalized_model_res}")
        
        # Add the additional data and timestamp
        # Adding model results and timestamp to the original random_data DataFrame
        random_data['model_res'] = [model_res[0].tolist()]
        random_data['Timestamp'] = [datetime.now().isoformat()]
        
        # Add the incremental ID
        random_data['Log_ID'] = [counter]  # Add the counter as a new field in the DataFrame
        counter += 1  # Increment the counter for the next iteration
        
        yield random_data.to_dict(orient='records')[0]  # Convert the DataFrame back to a dictionary
        
def delivery_report(err, msg):
    if err is not None:
        print(f"Message delivery failed: {err}")
    else:
        print(f"Message delivered to {msg.topic()} [{msg.partition()}]")

def produce_data_to_kafka(thread_name, producer, topic_name, csv_data,model):
    print(f"{thread_name} started.")
    rate_limit = 0.01  # 0.01 second sleep time to produce approximately 10 records per second
    for data in random_data_generator(csv_data):
        key = data.get("key_field", "test_arnaud")  # Get the key from data; replace 'key_field' with your key field
        json_data = json.dumps(data)  # Convert dict to JSON string
        while True:  # keep trying until message is sent
            try:
                producer.produce(topic_name, key=key, value=json_data, callback=delivery_report)
                producer.poll(0)  # Trigger delivery callbacks
                break  # exit while loop once message is sent
            except BufferError:
                print(f"{thread_name} - Local producer queue is full; waiting...")
                time.sleep(1)
        time.sleep(rate_limit)  # Sleep to control the rate
    print(f"{thread_name} stopped.")


if __name__ == "__main__":
    
    model = keras.models.load_model('it_threat_model.model')
    layer_name = 'dense'
    intermediate_layer_model = Model(inputs=model.input,
                                    outputs=model.get_layer(layer_name).output)
    
    
    # Initialize Kafka producer
    config = {
    'bootstrap.servers':'pkc-p11xm.us-east-1.aws.confluent.cloud:9092',
    'security.protocol':'SASL_SSL',
    'sasl.mechanisms':'PLAIN',
    'sasl.username':'BI5P76V7SKHRLNYM',
    'sasl.password':'UfHlbSseQMTKB4Rnh3YVBnXCGn605YBKLuTEkEEpLgUPwE1cWoFdXFFKB9MH97gY',
    'queue.buffering.max.messages':1000000
    }
    
    producer = Producer(config)

    # Read data from CSV
    csv_data = read_csv("result22022018_Vchanged.csv")
    
    # Kafka topic name
    topic_name = 'connections_siem_logs_V8'

    # Start 5 threads to produce data to Kafka
    for i in range(1):
        threading.Thread(target=produce_data_to_kafka, args=(f"Thread-{i}", producer, topic_name, csv_data, intermediate_layer_model)).start()