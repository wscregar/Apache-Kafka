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
