import pandas as pd
from minio import Minio
from io import BytesIO
from functools import reduce
import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.metrics import classification_report
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier


# Inisialisasi klien MinIO
minio_client = Minio(
    "localhost:9000", 
    access_key="minio_access_key",  
    secret_key="minio_secret_key",  
    secure=False  
)

bucket_name = "bank-data"  

objects = minio_client.list_objects(bucket_name, recursive=True)

dataframes = []  # List untuk menyimpan DataFrame

for obj in objects:
    if obj.object_name.endswith('.csv'):
        print(f"Reading file: {obj.object_name}")
        # Mengambil objek file dari MinIO
        data = minio_client.get_object(bucket_name, obj.object_name)
        # Membaca file CSV ke dalam DataFrame
        df = pd.read_csv(data)
        # Menambahkan DataFrame ke list
        dataframes.append(df)

# Menggabungkan semua DataFrame
if dataframes:
    df = pd.concat(dataframes, ignore_index=True)
    print(df.head())
else:
    print("No CSV files found in the bucket.")
    
print(f"Jumlah baris dan kolom: {df.shape}")



columns_to_drop = ['day', 'month', 'pdays', 'contact', 'previous']
data_dropped = df.drop(columns=columns_to_drop)


# Tentukan urutan kategori yang diinginkan
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
default_map = {
    "yes": 1,
    "no": 0
}
housing_map = {
    "yes": 1,
    "no": 0
}
loan_map = {
    "yes": 1,
    "no": 0
}
poutcome_map = {
    "unknown": 0,
    "other": 1,
    "failure": 2,
    "success": 3
}
y_map = {
    "yes": 1,
    "no": 0
}

# Fungsi untuk melakukan encoding manual
def encode_column(df, column_name, mapping):
    return df[column_name].map(mapping).fillna(-1)  # Menggunakan -1 untuk nilai yang tidak ada di mapping

# Terapkan mapping manual ke kolom-kolom yang diinginkan
data_dropped["job_encoded"] = encode_column(data_dropped, "job", job_map)
data_dropped["marital_encoded"] = encode_column(data_dropped, "marital", marital_map)
data_dropped["education_encoded"] = encode_column(data_dropped, "education", education_map)
data_dropped["default_encoded"] = encode_column(data_dropped, "default", default_map)
data_dropped["housing_encoded"] = encode_column(data_dropped, "housing", housing_map)
data_dropped["loan_encoded"] = encode_column(data_dropped, "loan", loan_map)
data_dropped["poutcome_encoded"] = encode_column(data_dropped, "poutcome", poutcome_map)
data_dropped["y_encoded"] = encode_column(data_dropped, "y", y_map)

# Tampilkan hasil encoding
data_dropped[[
    "job", "job_encoded",
    "marital", "marital_encoded",
    "education", "education_encoded",
    "default", "default_encoded",
    "housing", "housing_encoded",
    "loan", "loan_encoded",
    "poutcome", "poutcome_encoded",
    "y", "y_encoded"
]].head(15)

# Hapus kolom asli setelah encoding
columns_to_drop = ['job', 'marital', 'education', 'default', 'housing', 'loan', 'poutcome', 'y']
data_dropped = data_dropped.drop(columns=columns_to_drop)

# Ambil semua baris dengan y_encoded = 1
df_y1 = data_dropped[data_dropped['y_encoded'] == 1]
df_y0 = data_dropped[data_dropped['y_encoded'] == 0].sample(frac=0.4, random_state=52)
final_df = pd.concat([df_y1, df_y0])


# Tentukan kolom yang ingin dihapus
columns_to_drop = ['default_encoded','poutcome_encoded']  # Ganti dengan nama kolom yang ingin dihapus
final_df = final_df.drop(columns=columns_to_drop)
final_df.head(15)  


# Membuat kondisi untuk semua kolom numerik (nilai harus >= 0)
numeric_cols = [col for col in final_df.columns if final_df[col].dtype in ['float64', 'int64']]
# Membuat kondisi untuk memeriksa nilai >= 0 pada setiap kolom numerik
condition = [final_df[col] >= 0 for col in numeric_cols]
final_df = final_df[reduce(lambda a, b: a & b, condition)]

# Menampilkan statistik deskriptif untuk DataFrame setelah filter
stats = final_df.describe()

print(stats)


import pandas as pd

# Fungsi untuk menghitung batas outlier dan kuartil
def detect_outliers(df, col, custom_iqr=None):
    # Menghitung kuartil
    q1 = df[col].quantile(0.25)
    q3 = df[col].quantile(0.75)
    iqr = q3 - q1
    lower_bound = 0  # Batas bawah tetap nol
    upper_bound = q3 + (custom_iqr if custom_iqr else 0.8) * iqr  # Batas atas
    return q1, q3, lower_bound, upper_bound

def detect_outliers_age(df, col, custom_iqr=None):
    # Menghitung kuartil
    q1 = df[col].quantile(0.25)
    q3 = df[col].quantile(0.75)
    iqr = q3 - q1
    lower_bound = q1 - 1*iqr # Batas bawah tetap nol
    upper_bound = q3 + (custom_iqr if custom_iqr else 0.5) * iqr  # Batas atas
    return q1, q3, lower_bound, upper_bound


# Kolom yang ingin dianalisis
selected_cols = ["balance", "age", "campaign", "duration"]  # Gantilah dengan kolom lain yang relevan

# Pisahkan data berdasarkan nilai y_encoded
data_y0 = final_df[final_df['y_encoded'] == 0].copy()  # Baris dengan y_encoded == 0
data_y_other = final_df[final_df['y_encoded'] != 0].copy()  # Baris dengan y_encoded != 0

# Filter dan hapus outlier untuk baris di mana y_encoded == 0
for col in selected_cols:
    try:
        # Khusus untuk 'balance', tentukan custom_iqr yang lebih tinggi (misalnya 1.5 kali IQR)
        custom_iqr = 0.05 if col == "balance" else None
        
        # Hitung kuartil dan batas untuk mendeteksi outlier
        q1, q3, lb, ub = detect_outliers(data_y0, col, custom_iqr)
        print(f"Kolom: {col}")
        print(f"Q1 (Kuartil 1): {q1}")
        print(f"Q3 (Kuartil 3): {q3}")
        print(f"Batas bawah: {lb}, Batas atas: {ub}")

        # Filter outlier
        data_y0 = data_y0[(data_y0[col] >= lb) & (data_y0[col] <= ub)]
        
        # Menampilkan jumlah data yang tersisa
        print(f"Jumlah baris setelah menghapus outlier di kolom {col} (y_encoded == 0): {data_y0.shape[0]}\n")
    
    except Exception as e:
        print(f"Error dalam menghitung outlier untuk kolom {col}: {str(e)}")


# Tangani outlier untuk 'age' di data_y_other
try:
    # Hitung batas IQR untuk outlier kolom 'age'
    q1, q3, lb, ub = detect_outliers_age(data_y_other, "age")
    print("Menghapus outlier untuk data_y_other di kolom 'age'")
    print(f"Q1 (Kuartil 1): {q1}, Q3 (Kuartil 3): {q3}, Batas bawah: {lb}, Batas atas: {ub}")
    
    # Filter outlier untuk 'age' di data_y_other
    data_y_other = data_y_other[(data_y_other["age"] >= lb) & (data_y_other["age"] <= ub)]
    
    # Menampilkan jumlah data yang tersisa
    print(f"Jumlah baris setelah menghapus outlier di 'age' (y_encoded != 0): {data_y_other.shape[0]}\n")
except Exception as e:
    print(f"Error saat menangani outlier di kolom 'age': {str(e)}")

# Gabungkan kembali data yang telah difilter dengan data lain
filtered_data = pd.concat([data_y0, data_y_other])

# Tampilkan jumlah baris setelah semua penghapusan outlier
print(f"Jumlah baris setelah menghapus semua outlier: {filtered_data.shape[0]}")



df = filtered_data.copy()
# Menyiapkan kolom label
df = df.rename(columns={'y_encoded': 'label'})
# Pisahkan fitur dan label
X = df.drop(columns=['label'])  # Semua kolom kecuali 'label'
y = df['label']  # Kolom target 'label'

# Membagi data menjadi training dan testing set
X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.3,random_state=42)

# Membuat model Random Forest dengan parameter yang di-tuning
rf = RandomForestClassifier(
    n_estimators=200,            # Jumlah pohon keputusan
    max_depth=10,                # Kedalaman maksimal pohon
    min_samples_split=10,        # Jumlah minimum sampel yang diperlukan untuk membagi node
    min_samples_leaf=4,          # Jumlah minimum sampel di daun
    max_features='sqrt',         # Menggunakan jumlah fitur yang lebih kecil untuk membangun setiap pohon
    random_state=42,             # Untuk hasil yang konsisten
    n_jobs=-1                    # Menggunakan semua core CPU untuk perhitungan paralel
)

# Melatih model menggunakan data training
rf_model = rf.fit(X_train, y_train)

# Melakukan prediksi menggunakan data testing
y_pred = rf_model.predict(X_test)

# Menampilkan laporan evaluasi
from sklearn.metrics import classification_report
print(classification_report(y_test, y_pred))
import joblib
import os
from datetime import datetime

# Direktori tempat menyimpan model
model_directory = r'C:\its\SEM 5\Big Data\FP\Data Lakehouse\MODELLING'

# Buat nama file dengan timestamp
model_filename = f"RF_{datetime.now().strftime('%Y%m%d_%H%M%S')}.joblib"
model_path = os.path.join(model_directory, model_filename)

# Simpan model
joblib.dump(rf_model, model_path)
print(f"Model baru disimpan di: {model_path}")

