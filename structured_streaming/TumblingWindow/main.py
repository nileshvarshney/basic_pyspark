from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import from_json, col, to_timestamp, window, expr, sum
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

from utils.logger import Log4j


if __name__ == "__main__":
    spark = SparkSession.builder\
            .master("local[3]")\
            .config("spark.streaming.stopGracefullyOnShutdown", "true")\
            .config("spark.sql.shuffle.partitions", 2)\
            .appName("Tumbling Window")\
            .getOrCreate()

    logger = Log4j(spark)
    logger.info("Tumbling Window Start")
    stock_schema = StructType([
        StructField("CreatedTime", StringType()),
        StructField("Type", StringType()),
        StructField("Amount", IntegerType()),
        StructField("BrokerCode", StringType())
    ])

    kafka_df = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:9092")\
        .option("subscribe", "trades")\
        .option("startingOffsets", "earliest")\
        .load()

    value_df = kafka_df.select(from_json(col("value").cast("string"), stock_schema).alias("value"))

    trade_df = value_df.select("value.*") \
        .withColumn("CreatedTime", to_timestamp(col("CreatedTime"), "yyyy-MM-dd HH:mm:ss")) \
        .withColumn("Buy", expr("case when Type == 'BUY' then Amount else 0 end")) \
        .withColumn("Sell", expr("case when Type == 'SELL' then Amount else 0 end"))

    window_agg_df = trade_df \
        .groupBy(  # col("BrokerCode"),
         window(col("CreatedTime"), "15 minute")) \
        .agg(expr("sum(Buy) as TotalBuy"),
             expr("sum(Sell) as TotalSell"))

    output_df = window_agg_df.select("window.start", "window.end", "TotalBuy", "TotalSell")
    logger.info('output_df :' + output_df.schema.simpleString())

    '''
    running_output_window = Window.orderBy("end")\
        .rowsBetween(Window.unboundedPreceding, Window.currentRow)

    final_output_df = output_df\
        .withColumn("RToalBuy", sum("TotalBuy").over(running_output_window))\
        .withColumn("RTotalSell", sum("TotalSell").over(running_output_window))\
        .withColumn("NetValue", expr("RToalBuy - RTotalSell"))
    '''

    query = output_df.writeStream.format("console")\
        .outputMode("update")\
        .option("checkpointLocation", 'chk-point-dir')\
        .trigger(processingTime="1 minute")\
        .start()

    query.awaitTermination()
    logger.info("Tumbling Window End")
