from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, current_timestamp, window
from pyspark.sql.types import StructType, StringType, IntegerType

# 1. Inisialisasi Spark
spark = SparkSession.builder \
    .appName("Gudang Monitoring") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# 2. Skema
schema_suhu = StructType().add("gudang_id", StringType()).add("suhu", IntegerType())
schema_kelembaban = StructType().add("gudang_id", StringType()).add("kelembaban", IntegerType())

# 3. Baca dari Kafka
df_suhu = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-suhu-gudang") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema_suhu).alias("data")) \
    .select("data.*")

df_kelembaban = spark.readStream.format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "sensor-kelembaban-gudang") \
    .load() \
    .selectExpr("CAST(value AS STRING) as json") \
    .select(from_json(col("json"), schema_kelembaban).alias("data")) \
    .select("data.*")

# 4. Tambahkan timestamp ke masing-masing stream
df_suhu = df_suhu.withColumn("timestamp", current_timestamp())
df_kelembaban = df_kelembaban.withColumn("timestamp", current_timestamp())

# 5. Join berdasarkan gudang_id dan window waktu
joined = df_suhu.join(
    df_kelembaban,
    on="gudang_id"
).withWatermark("timestamp", "10 seconds") \
 .groupBy(window("timestamp", "10 seconds"), "gudang_id") \
 .agg({"suhu": "max", "kelembaban": "max"}) \
 .withColumnRenamed("max(suhu)", "suhu") \
 .withColumnRenamed("max(kelembaban)", "kelembaban")

# 6. Tambahkan kolom status
output = joined.withColumn("status", 
    when((col("suhu") > 80) & (col("kelembaban") > 70), "Bahaya tinggi! Barang berisiko rusak")
    .when(col("suhu") > 80, "Suhu tinggi, kelembaban normal")
    .when(col("kelembaban") > 70, "Kelembaban tinggi, suhu aman")
    .otherwise("Aman"))

# 7. Output ke console
query = output.writeStream.outputMode("update").format("console").start()
query.awaitTermination()



  
