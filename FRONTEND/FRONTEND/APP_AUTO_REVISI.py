import joblib
import os
from flask import Flask, render_template, request
import pandas as pd
import threading
import time

app = Flask(__name__)

# Direktori tempat model disimpan
model_directory = r'C:\its\SEM 5\Big Data\FP\Data Lakehouse\MODELLING'

# Fungsi untuk mencari model terbaru di direktori
def get_latest_model(model_dir):
    # Cari semua file dalam direktori yang berakhiran ".joblib"
    model_files = [f for f in os.listdir(model_dir) if f.endswith('.joblib')]
    if not model_files:
        raise FileNotFoundError("Tidak ada model ditemukan di direktori.")
    
    # Urutkan file berdasarkan waktu modifikasi
    model_files.sort(key=lambda x: os.path.getmtime(os.path.join(model_dir, x)), reverse=True)
    latest_model_path = os.path.join(model_dir, model_files[0])
    return latest_model_path

# Fungsi untuk memuat model terbaru dan memeriksa setiap beberapa detik
def load_latest_model():
    global model  # Memastikan variabel model global dapat diupdate
    while True:
        try:
            latest_model_path = get_latest_model(model_directory)  # Cek model terbaru
            model = joblib.load(latest_model_path)
            print(f"Model terbaru yang digunakan: {latest_model_path}")
        except Exception as e:
            print(f"Terjadi kesalahan saat memuat model: {e}")
        time.sleep(60)  # Cek setiap 60 detik

# Muat model pertama kali saat aplikasi dimulai
try:
    latest_model_path = get_latest_model(model_directory)
    model = joblib.load(latest_model_path)
    print(f"Model sebelumnya : {latest_model_path}")
except Exception as e:
    print(f"Terjadi kesalahan saat memuat model pertama kali: {e}")
    model = None  # Atau beri fallback model default jika ada

# Define the feature maps for encoding
job_map = {
    "student": 2,
    "admin.": 5,
    "management": 10,
    "unemployed": 1,
    "retired": 3,
    "housemaid": 4,
    "entrepreneur": 11,
    "blue-collar": 7,
    "self-employed": 6,
    "technician": 9,
    "services": 8,
    "unknown": 0
}

marital_map = {
    "married": 1,
    "divorced": 2,
    "single": 0
}

education_map = {
    "unknown": 0,
    "primary": 1,
    "secondary": 2,
    "tertiary": 3
}

housing_map = {
    "yes": 1,
    "no": 0
}

loan_map = {
    "yes": 1,
    "no": 0
}

y_map = {
    "yes":1,
    "no":0
}

# Encode features manually using the above mappings
def encode_column(df, column_name, mapping):
    return df[column_name].map(mapping).fillna(-1)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/form')
def form():
    return render_template('form.html')

@app.route('/about')
def about():
    return render_template('about.html')

@app.route('/submit_form', methods=['POST'])
def submit_form():
    # Capture form data
    usia = request.form['usia']
    jenis_pekerjaan = request.form['jenis_pekerjaan']
    status_pernikahan = request.form['status_pernikahan']
    pendidikan = request.form['pendidikan']
    saldo_rata_rata = request.form['saldo_rata_rata']
    pinjaman_rumah = request.form['pinjaman_rumah']
    pinjaman_pribadi = request.form['pinjaman_pribadi']
    durasi_percakapan = request.form['durasi_percakapan']
    jumlah_kontak = request.form['jumlah_kontak']
    
    # Prepare the data into a DataFrame for prediction
    data = pd.DataFrame({
        'age': [usia],
        'job': [jenis_pekerjaan],
        'marital': [status_pernikahan],
        'education': [pendidikan],
        'balance': [saldo_rata_rata],
        'housing': [pinjaman_rumah], 
        'loan': [pinjaman_pribadi], 
        'duration': [durasi_percakapan], 
        'campaign': [jumlah_kontak], 
    })
    
    # Apply the encoding to the DataFrame
    data["job_encoded"] = encode_column(data, "job", job_map)
    data["marital_encoded"] = encode_column(data, "marital", marital_map)
    data["education_encoded"] = encode_column(data, "education", education_map)
    data["housing_encoded"] = encode_column(data, "housing", housing_map)
    data["loan_encoded"] = encode_column(data, "loan", loan_map)

    # Select the encoded columns for prediction
    features = data[[
        'age', 'balance', 'duration','campaign', 'job_encoded', 'marital_encoded', 'education_encoded',
        'housing_encoded', 'loan_encoded'
    ]]
    
    # Make a prediction
    if model is not None:
        prediction = model.predict(features)[0]
    else:
        prediction = None

    # Convert the prediction back to 'Yes' or 'No'
    if prediction is not None:
        hasil = "Potential" if prediction == 1 else "No Potential"
        color = "blue" if hasil == "Potential" else "red"
    else:
        hasil = "Model tidak tersedia"
        color = "gray"
    
    return render_template('hasil.html', hasil=hasil, color=color)

if __name__ == '__main__':
    # Memulai thread polling model terbaru
    thread = threading.Thread(target=load_latest_model)
    thread.daemon = True  # Daemon thread agar thread mati ketika aplikasi berhenti
    thread.start()

    app.run(debug=True)
