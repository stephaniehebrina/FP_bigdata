import csv
import os
from kafka import KafkaProducer
import json
from datetime import datetime  # Impor untuk membuat timestamp

# Inisialisasi Kafka Producer
producer = KafkaProducer(
    bootstrap_servers='localhost:9092',
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Kafka topic
topic_name = "bank-topic"

# List untuk menyimpan data sementara
buffer = []

# Direktori tujuan untuk menyimpan CSV
csv_directory = r"C:\its\SEM 5\Big Data\FP\Data Lakehouse\lakehouse-3\lakehouse-3\streaming-data\day_csv"

# Membuat direktori jika belum ada
if not os.path.exists(csv_directory):
    os.makedirs(csv_directory)
    print(f"Direktori {csv_directory} dibuat.")

print(f"Producer Kafka berjalan. Masukkan data. Setelah beberap data terkumpul, akan disimpan ke CSV dan dikirim ke Kafka.")
print("Tekan Ctrl+C untuk berhenti.\n")

try:
    while True:
        # Input data baru dari pengguna
        print("Masukkan data baru:")
        age = int(input("Age: "))
        job = input("Job: ")
        marital = input("Marital: ")
        education = input("Education: ")
        default = input("Default (yes/no): ")
        balance = float(input("Balance: "))
        housing = input("Housing (yes/no): ")
        loan = input("Loan (yes/no): ")
        contact = input("Contact: ")
        day = int(input("Day: "))
        month = input("Month: ")
        duration = int(input("Duration: "))
        campaign = int(input("Campaign: "))
        pdays = int(input("Pdays: "))
        previous = int(input("Previous: "))
        poutcome = input("Poutcome: ")
        y = input("Y (yes/no): ")

        # Gabungkan input menjadi dictionary
        new_data = {
            "age": age,
            "job": job,
            "marital": marital,
            "education": education,
            "default": default,
            "balance": balance,
            "housing": housing,
            "loan": loan,
            "contact": contact,
            "day": day,
            "month": month,
            "duration": duration,
            "campaign": campaign,
            "pdays": pdays,
            "previous": previous,
            "poutcome": poutcome,
            "y": y
        }

        # Tambahkan data ke buffer
        buffer.append(new_data)
        print(f"Data ditambahkan ke buffer. Total data di buffer: {len(buffer)}\n")

        # Jika buffer mencapai 2 data, tulis ke CSV dengan nama berdasarkan timestamp
        if len(buffer) == 2:
            # Buat nama file berdasarkan timestamp
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            csv_file = os.path.join(csv_directory, f"data_{timestamp}.csv")  # Path lengkap ke file
            
            # Tulis buffer ke file CSV
            with open(csv_file, mode='w', newline='') as file:
                writer = csv.DictWriter(file, fieldnames=new_data.keys())
                writer.writeheader()  # Tulis header hanya sekali
                writer.writerows(buffer)

            print(f"Data disimpan ke file CSV: {csv_file}")

            # Kirim data dari CSV ke Kafka topic
            with open(csv_file, 'r') as file:
                csv_content = file.read()
                producer.send(topic_name, value={"file_content": csv_content})
                print(f"File CSV dikirim ke Kafka topic '{topic_name}'")

            # Kosongkan buffer setelah pengiriman
            buffer.clear()

except KeyboardInterrupt:
    print("\nProducer dihentikan oleh pengguna.")

finally:
    # Tutup Kafka Producer dengan baik
    producer.flush()
    producer.close()
    print("Kafka Producer ditutup.")
