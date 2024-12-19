from kafka import KafkaProducer
import json
import time
import pandas as pd
from datetime import datetime

# Serializer untuk mengonversi data ke format JSON
def json_serializer(data):
    return json.dumps(data).encode('utf-8')

# Kafka producer setup
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],  # Sesuaikan dengan alamat Kafka Anda
    value_serializer=json_serializer
)

# Membaca file CSV
df = pd.read_csv('Bank-full.csv')

# Fungsi untuk men-stream data per batch
def stream_data_in_batches(batch_size=9042):
    # Membagi DataFrame menjadi batch
    total_rows = len(df)
    num_batches = total_rows // batch_size
    for i in range(num_batches):
        batch = df[i * batch_size : (i + 1) * batch_size]  # Mengambil batch sesuai indeks
        for _, row in batch.iterrows():
            data = row.to_dict()  # Mengonversi baris ke dictionary
            producer.send('bank-topic', value=data)  # Mengirim data ke Kafka topic
            print(f"Sent data: {data}")  # Menampilkan data yang dikirim
            time.sleep(0.005)  # Jeda 0.5 detik per pengiriman
        print(f"Completed sending batch {i + 1}/{num_batches}")

if __name__ == "__main__":
    stream_data_in_batches(batch_size=9042)
