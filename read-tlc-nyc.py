# Import
from pyspark.sql import SparkSession

# Create SparrkSession
spark = SparkSession.builder.appName('Read TLC NYC').getOrCreate()
df_nyc_tlc = spark.read.parquet("oci://hosted-ds-datasets@bigdatadatasciencelarge/nyc_tlc/201[8,9]/**/data.parquet", header=False, inferSchema=True)

df_nyc_tlc.printSchema()
df_nyc_tlc.count()
df_nyc_tlc.show(10)

from pyspark.sql.functions import sum,avg,max
df_nyc_tlc.groupBy("vendor_id") \
    .agg(sum("Total_amount"), \
         avg("Passenger_count").alias("Avg_Passengers"), \
         sum("Passenger_count"), \
         max("Passenger_count").alias("Max_Passengers"), \
         max("Total_amount").alias("Max_Amount"), \
         avg("Total_amount").alias("Avg_Amount")
     ) \
    .show(truncate=False)

df_nyc_tlc.createOrReplaceTempView("tlc")
spark.sql("SELECT DISTINCT vendor_id, payment_type FROM tlc").show()