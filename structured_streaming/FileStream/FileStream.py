from pyspark.sql import SparkSession
from utils.logger import Log4j
from pyspark.sql.functions import explode, expr

if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("File Stream") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnStutdown", "true") \
        .config("spark.sql.streaming.schemaInference","true")\
        .getOrCreate()

    logger = Log4j(spark)
    raw_df = spark.readStream\
        .format("json")\
        .option("path", "input/")\
        .option("maxFilesPerTrigger", "1")\
        .load()

    exploded_df = raw_df.selectExpr("InvoiceNumber","CreatedTime","StoreID","PosID",
                                    "CustomerType","PaymentMethod","DeliveryType",
                                    "DeliveryAddress.City", "DeliveryAddress.State","DeliveryAddress.PinCode",
                                    "explode(InvoiceLineItems) as lineItems")
    logger.info('explded_df schema :' + exploded_df.schema.simpleString() )
    flatten_df = exploded_df\
        .withColumn("ItemCode", expr("lineItems.ItemCode")) \
        .withColumn("ItemDescription", expr("lineItems.ItemDescription")) \
        .withColumn("ItemPrice", expr("lineItems.ItemPrice")) \
        .withColumn("ItemQty", expr("lineItems.ItemQty")) \
        .withColumn("TotalValue", expr("lineItems.TotalValue"))\
        .drop("lineItems")

    logger.info('flatten_df schema :' + flatten_df.schema.simpleString())

    invoice_writer_query = flatten_df.writeStream\
        .format("json")\
        .option("checkpointLocation", "chk-point-dir")\
        .option("path", "output")\
        .outputMode("append")\
        .trigger(processingTime="1 minute")\
        .queryName("Flatten Invoice Writter")\
        .start()

    logger.info("Flatten Invoice Writter started")
    invoice_writer_query.awaitTermination()