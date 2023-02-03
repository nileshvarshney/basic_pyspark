from pyspark.sql import SparkSession
from utils.logger import Log4j
from pyspark.sql.functions import split, explode

if __name__ == "__main__":
    spark = SparkSession.builder\
        .appName("Streaming Word Cound") \
        .master("local[3]") \
        .config("spark.streaming.stopGracefullyOnStutdown", "true") \
        .config("spark.sql.shuffle.partitions", 3) \
        .getOrCreate()

    logger = Log4j(spark)
    line_df = spark.readStream\
        .format("socket")\
        .option("host", "localhost")\
        .option("port", 9999)\
        .load()

    logger.info('Streaming Dataframe ' + line_df.schema.simpleString())
    words_df = line_df.selectExpr("explode(split(value,' ')) as word")
    count_df= words_df.groupBy("word").count()
    logger.info('Streaming Dataframe ' + count_df.schema.simpleString())

    word_count_df = count_df.writeStream\
        .format("console")\
        .option("checkpointLocation", "chk-point-dir")\
        .outputMode("complete")\
        .start()

    logger.info('Listerning to localhost:9999')
    word_count_df.awaitTermination()
