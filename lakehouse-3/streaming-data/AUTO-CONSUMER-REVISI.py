import logging
import json
import os
from kafka import KafkaConsumer
from minio import Minio
from datetime import datetime

# Konfigurasi logging
logging.basicConfig(level=logging.INFO)

# Inisialisasi MinIO client
minio_client = Minio(
    "localhost:9000",
    access_key="minio_access_key",
    secret_key="minio_secret_key",
    secure=False
)

# Pastikan bucket MinIO tersedia
bucket_name = "bank-data"
if not minio_client.bucket_exists(bucket_name):
    minio_client.make_bucket(bucket_name)
    logging.info(f"Bucket '{bucket_name}' created.")
else:
    logging.info(f"Bucket '{bucket_name}' already exists.")

# Inisialisasi Kafka Consumer
consumer = KafkaConsumer(
    'bank-topic',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='latest',
    enable_auto_commit=True,
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

logging.info("Kafka Consumer berjalan...")

# Konsumsi pesan dari Kafka
try:
    for message in consumer:
        # Baca isi pesan
        data = message.value

        # Periksa apakah pesan memiliki konten file CSV
        if "file_content" in data:
            file_content = data["file_content"]

            # Buat nama file unik berdasarkan timestamp
            file_name = f"batch_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv"

            # Simpan file sementara
            with open(file_name, "w") as temp_file:
                temp_file.write(file_content)

            logging.info(f"File sementara '{file_name}' dibuat.")

            # Upload file ke MinIO
            minio_client.fput_object(
                bucket_name,  # Nama bucket
                file_name,    # Nama file di MinIO
                file_name     # Lokasi file lokal
            )
            logging.info(f"File '{file_name}' berhasil diupload ke bucket '{bucket_name}' di MinIO.")

            # Hapus file sementara setelah upload
            try:
                os.remove(file_name)
                logging.info(f"File sementara '{file_name}' dihapus.")
            except Exception as e:
                logging.warning(f"Gagal menghapus file sementara '{file_name}': {e}")

except KeyboardInterrupt:
    logging.info("Consumer dihentikan oleh pengguna.")

except Exception as e:
    logging.error(f"Terjadi kesalahan: {e}")

finally:
    logging.info("Menutup Kafka Consumer.")
