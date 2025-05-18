# Dokumentasi Pengerjaan Tugas Problem Based Learning : Apache Kafka

Wira Samudra Siregar (5027231041)

Big Data (B)

## Latar Belakang Masalah

Sebuah perusahaan logistik mengelola beberapa gudang penyimpanan yang menyimpan barang sensitif seperti makanan, obat-obatan, dan elektronik. Untuk menjaga kualitas penyimpanan, gudang-gudang tersebut dilengkapi dengan dua jenis sensor:

* Sensor Suhu

* Sensor Kelembaban

Sensor akan mengirimkan data setiap detik. Perusahaan ingin memantau kondisi gudang secara real-time untuk mencegah kerusakan barang akibat suhu terlalu tinggi atau kelembaban berlebih.

## Tugas 

### 1. Buat Topik Kafka

Buat dua topik di Apache Kafka:

* sensor-suhu-gudang:

``/usr/bin/kafka-topics --create --topic sensor-suhu-gudang --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1``

Screenshoot:
![image](https://github.com/user-attachments/assets/673529d9-e441-410f-9c22-1719352c56dc)


* sensor-kelembaban-gudang

``/usr/bin/kafka-topics --create --topic sensor-kelembaban-gudang --bootstrap-server localhost:9092 --partitions 1 --replication-factor 1``

Screenshoot:
![image](https://github.com/user-attachments/assets/d6c3468c-923a-497c-87cc-53f85295bd96)

Cek list topik:

``kafka-topics.sh --list --bootstrap-server localhost:9092``

Screenshoot:

![image](https://github.com/user-attachments/assets/b93490f9-35e6-42de-baa4-0f57a9c0c3a8)


Topik ini akan digunakan untuk menerima data dari masing-masing sensor secara real-time.


### 2. Simulasikan Data Sensor (Producer Kafka)

Buat dua Kafka producer terpisah:

a. Producer Suhu
Kode: producer_suhu.py
![image](https://github.com/user-attachments/assets/bad2d52b-57cd-411e-ba00-0fc8f4054b99)

Screenshoot:

![image](https://github.com/user-attachments/assets/84c8cc3a-de88-46ad-9dc5-7e08bc601bef)


b. Produser Kelembaban
Kode: producer_kelembaban.py
![image](https://github.com/user-attachments/assets/cfbf34ae-b2fb-44a7-8dc9-048bd72bf8a3)

Screenshoot:

![image](https://github.com/user-attachments/assets/5edf411e-5159-47c6-bf16-2872890f66d1)



### 3. Konsumsi dan Olah Data dengan PySpark

a. Buat PySpark Consumer

Konsumsi data dari kedua topik Kafka.

Kode: 

b. Lakukan Filtering:
Suhu > 80°C → tampilkan sebagai peringatan suhu tinggi

Kelembaban > 70% → tampilkan sebagai peringatan kelembaban tinggi

Contoh Output:
[Peringatan Suhu Tinggi]
Gudang G2: Suhu 85°C

[Peringatan Kelembaban Tinggi]
Gudang G3: Kelembaban 74%
4. Gabungkan Stream dari Dua Sensor
Lakukan join antar dua stream berdasarkan gudang_id dan window waktu (misalnya 10 detik) untuk mendeteksi kondisi bahaya ganda.

c. Buat Peringatan Gabungan:
Jika ditemukan suhu > 80°C dan kelembaban > 70% pada gudang yang sama, tampilkan peringatan kritis.

## Output Gabungan:
[PERINGATAN KRITIS]
Gudang G1:
- Suhu: 84°C
- Kelembaban: 73%
- Status: Bahaya tinggi! Barang berisiko rusak

Gudang G2:
- Suhu: 78°C
- Kelembaban: 68%
- Status: Aman

Gudang G3:
- Suhu: 85°C
- Kelembaban: 65%
- Status: Suhu tinggi, kelembaban normal

Gudang G4:
- Suhu: 79°C
- Kelembaban: 75%
- Status: Kelembaban tinggi, suhu aman

### Penyelesaian Soal 1
