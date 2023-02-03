from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, expr
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType, IntegerType, ArrayType

from utils.logger import Log4j


if __name__ == "__main__":
    spark = SparkSession.builder\
        .master("local[3]")\
        .appName("Kafka Streaming")\
        .config("spark.streaming.stopGracefullyOnShutdown", "true")\
        .getOrCreate()
        #  #.config("spark.jars.packages","org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.1")\

    logger = Log4j(spark)
    logger.info('Kafka Streaming start')

    schema = StructType([
        StructField("InvoiceNumber", StringType()),
        StructField("CreatedTime", LongType()),
        StructField("StoreID", StringType()),
        StructField("PosID", StringType()),
        StructField("CashierID", StringType()),
        StructField("CustomerType", StringType()),
        StructField("CustomerCardNo", StringType()),
        StructField("TotalAmount", DoubleType()),
        StructField("NumberOfItems", IntegerType()),
        StructField("PaymentMethod", StringType()),
        StructField("CGST", DoubleType()),
        StructField("SGST", DoubleType()),
        StructField("CESS", DoubleType()),
        StructField("DeliveryType", StringType()),
        StructField("DeliveryAddress", StructType([
            StructField("AddressLine", StringType()),
            StructField("City", StringType()),
            StructField("State", StringType()),
            StructField("PinCode", StringType()),
            StructField("ContactNumber", StringType())
        ])),
        StructField("InvoiceLineItems", ArrayType(StructType([
            StructField("ItemCode", StringType()),
            StructField("ItemDescription", StringType()),
            StructField("ItemPrice", DoubleType()),
            StructField("ItemQty", IntegerType()),
            StructField("TotalValue", DoubleType())
        ]))),
    ])

    kafka_df = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "localhost:9092") \
        .option("subscribe", "invoices") \
        .option("startingOffsets", "earliest") \
        .load()

    # since kakfa contains key value pair in binary format that need to convert to string
    value_df = kafka_df.select(from_json(col("value").cast("string"), schema).alias("value"))
    logger.info('value_df schema :' + value_df.schema.simpleString())

    exploded_df = value_df.selectExpr("value.InvoiceNumber",
                                      "value.CreatedTime",
                                      "value.StoreID",
                                      "value.PosID",
                                      "value.CustomerType",
                                      "value.PaymentMethod",
                                      "value.DeliveryType",
                                      "value.DeliveryAddress.City",
                                      "value.DeliveryAddress.State",
                                      "value.DeliveryAddress.PinCode",
                                      "explode(value.InvoiceLineItems) as lineItems")
    logger.info('explded_df schema :' + exploded_df.schema.simpleString())

    flatten_df = exploded_df \
        .withColumn("ItemCode", expr("lineItems.ItemCode")) \
        .withColumn("ItemDescription", expr("lineItems.ItemDescription")) \
        .withColumn("ItemPrice", expr("lineItems.ItemPrice")) \
        .withColumn("ItemQty", expr("lineItems.ItemQty")) \
        .withColumn("TotalValue", expr("lineItems.TotalValue")) \
        .drop("lineItems")

    logger.info('flatten_df schema :' + flatten_df.schema.simpleString())
    #  flatten_df.printSchema()

    invoice_writer_query = flatten_df.writeStream\
            .format("json")\
            .outputMode("append")\
            .queryName("flatten invoice writter")\
            .option("path", "output")\
            .option("checkpointLocation","chk-point-dir")\
            .trigger(processingTime = "1 minute")\
            .start()

    logger.info('Listening to Kafka')
    invoice_writer_query.awaitTermination()