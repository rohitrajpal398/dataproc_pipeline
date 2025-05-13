from pyspark.sql import SparkSession

def main():
    spark = SparkSession.builder.appName("SalesPipeline").getOrCreate()
    df = spark.read.csv("gs://sample-data-for-pract1/orders.csv", header=True, inferSchema=True)
    df = df.withColumn("total", df.quantity * df.price)
    df.write.format("bigquery") \
        .option("table", "airy-actor-457907-a8.sales_dataset.sales_summary") \
        .option("temporaryGcsBucket", "sample-data-for-pract1") \
        .mode("overwrite") \
        .save()

if __name__ == "__main__":
    main()