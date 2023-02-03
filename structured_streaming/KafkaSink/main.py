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

    notification_df = value_df.selectExpr("value.InvoiceNumber", "value.CustomerCardNo","value.TotalAmount")\
           .withColumn('LoyalityPointEarned', (expr("round(TotalAmount * 0.2)").cast("integer")))

    logger.info('notification_df schema :' + notification_df.schema.simpleString())

    kafka_notification_df = notification_df.selectExpr("InvoiceNumber as key", """
        to_json(named_struct(
            'CustomerCardNo', CustomerCardNo,
            'TotalAmount', TotalAmount,
            'LoyalityPointEarned', LoyalityPointEarned
        )) as value
    """)

    logger.info('kafka_notification_df schema :' + kafka_notification_df.schema.simpleString())

    notification_writer_query = kafka_notification_df.writeStream\
            .format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9092") \
            .option("topic", "notifications") \
            .outputMode("append")\
            .queryName("kafka notification writter")\
            .option("checkpointLocation","chk-point-dir")\
            .start()

    logger.info('Reading and Writting to Kafka')
    notification_writer_query.awaitTermination()