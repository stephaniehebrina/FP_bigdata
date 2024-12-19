import subprocess
import time

# Jalankan AUTO-CONSUMER.py terlebih dahulu
def run_auto_consumer():
    print("Menjalankan AUTO-CONSUMER-REVISI.py...")
    subprocess.Popen(["python", "C:\its\SEM 5\Big Data\FP\Data Lakehouse\lakehouse-3\lakehouse-3\streaming-data\AUTO-CONSUMER-REVISI.py"])
    print("AUTO-CONSUMER.py dijalankan.")

# Jalankan model.py setiap 60 detik
def loop_model():
    print("Mulai looping MODEL_REVISI.py setiap 60 detik...")
    while True:
        print("Menjalankan MODEL_REVISI.py...")
        subprocess.run(["python", "C:\its\SEM 5\Big Data\FP\Data Lakehouse\MODELLING\MODEL_REVISI.py"])
        print("MODEL_REVISI.py telah selesai dijalankan.")
        time.sleep(60)  # Tunggu 60 detik

# Jalankan app.py yang selalu diperbarui
def run_app():
    print("Menjalankan APP_AUTO_REVISI.py...")
    while True:
        subprocess.run(["python", "C:\its\SEM 5\Big Data\FP\Data Lakehouse\FRONTEND\FRONTEND\APP_AUTO_REVISI.py"])
        print("APP_AUTO_REVISI.py telah selesai dijalankan.")
        time.sleep(1)  # Tunggu sebentar sebelum menjalankan ulang app.py

def main():
    # Jalankan AUTO-CONSUMER.py
    run_auto_consumer()
    
    # Jalankan model.py dalam loop terpisah
    import threading
    model_thread = threading.Thread(target=loop_model)
    model_thread.daemon = True
    model_thread.start()
    
    # Jalankan app.py
    run_app()

if __name__ == "__main__":
    main()
