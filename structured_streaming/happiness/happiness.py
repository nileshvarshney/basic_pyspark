from pyspark.sql import SparkSession
from utils.logger import Log4j
from utils.transformation import calculate_percentage_change, read_streaming_data


def main():
    spark = SparkSession.builder\
        .master("local[3]") \
        .config("spark.sql.shuffle.partitions", 3) \
        .config("spark.sql.streaming.schemaInference", "true")\
        .config("spark.streaming.stopGracefullyOnShutdown", "true")\
        .appName("stock_price").getOrCreate()

    logger = Log4j(spark)

    streaming_df = read_streaming_data(spark, filepath="data/stock_data/*.csv", max_file_per_trigger=3)

    logger.info('input data schema' + streaming_df.schema.simpleString())

    final_df = calculate_percentage_change(streaming_df)

    query = final_df.writeStream\
        .outputMode("complete")\
        .format('console')\
        .trigger(processingTime="1 minute")\
        .start()

    logger.info('stock_price processing end..')
    query.awaitTermination()


if __name__ == "__main__":
    main()