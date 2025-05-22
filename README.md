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
    
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import from_json, col
    from pyspark.sql.types import StructType, StringType, IntegerType

    # Buat Spark Session
    spark = SparkSession.builder \
    .appName("KafkaSensorConsumer") \
    .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Schema masing-masing data
    schema_suhu = StructType().add("gudang_id", StringType()).add("suhu", IntegerType())
    schema_kelembaban = StructType().add("gudang_id", StringType()).add("kelembaban", IntegerType())

    # Stream suhu
    df_suhu = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-suhu-gudang") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema_suhu).alias("data")) \
    .select("data.*")

    # Stream kelembaban
    df_kelembaban = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-kelembaban-gudang") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema_kelembaban).alias("data")) \
    .select("data.*")

    # Filter suhu tinggi
    peringatan_suhu = df_suhu.filter(col("suhu") > 80)

    # Filter kelembaban tinggi
    peringatan_kelembaban = df_kelembaban.filter(col("kelembaban") > 70)

    # Tampilkan ke console
    query_suhu = peringatan_suhu.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

    query_kelembaban = peringatan_kelembaban.writeStream \
    .outputMode("append") \
    .format("console") \
    .option("truncate", False) \
    .start()

    query_suhu.awaitTermination()
    query_kelembaban.awaitTermination()
    
b. Lakukan Filtering:

Suhu > 80°C → tampilkan sebagai peringatan suhu tinggi

Kelembaban > 70% → tampilkan sebagai peringatan kelembaban tinggi

Kode:

    # Filter suhu tinggi
    peringatan_suhu = df_suhu.filter(col("suhu") > 80)

    # Filter kelembaban tinggi
    peringatan_kelembaban = df_kelembaban.filter(col("kelembaban") > 70)

### 4. Gabungkan Stream dari Dua Sensor
Lakukan join antar dua stream berdasarkan gudang_id dan window waktu (misalnya 10 detik) untuk mendeteksi kondisi bahaya ganda.

Kode:

    # Stream suhu
    df_suhu = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-suhu-gudang") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema_suhu).alias("data")) \
    .select("data.*")

    # Stream kelembaban
    df_kelembaban = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-kelembaban-gudang") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema_kelembaban).alias("data")) \
    .select("data.*")
    
c. Buat Peringatan Gabungan:
Jika ditemukan suhu > 80°C dan kelembaban > 70% pada gudang yang sama, tampilkan peringatan kritis.

## Output:

![image](https://github.com/user-attachments/assets/07ad5178-ea0f-432c-99e1-5874f031d68a)

![image](https://github.com/user-attachments/assets/39155790-12f8-48f5-aad2-25865c1126c4)

![image](https://github.com/user-attachments/assets/2096bb26-6ee5-4552-b304-78a0252bacec)


### Penjelasan:

Masalah yang terjadi saat menjalankan pyspark_consumer.py di container Docker Bitnami Spark adalah kegagalan Spark dalam menginisialisasi Java gateway karena direktori .ivy2 yang digunakan untuk mengelola dependency Maven tidak memiliki path absolut yang valid. Hal ini biasanya disebabkan oleh tidak adanya atau tidak validnya variabel lingkungan HOME dalam container, sehingga Spark dan Ivy tidak tahu di mana harus menyimpan cache dependency-nya. Akibatnya, proses JVM gagal berjalan dan menyebabkan Python SparkContext tidak dapat terhubung ke Java gateway, menghasilkan error JAVA_GATEWAY_EXITED. Untuk mengatasi masalah ini, perlu memastikan bahwa environment variable HOME di-set ke path absolut yang valid seperti /tmp dalam container, atau menjalankan script dengan spark-submit yang sudah mengatur environment dengan benar, sehingga Spark dapat mengakses direktori ivy dengan path yang sesuai dan Java gateway bisa berjalan lancar.

