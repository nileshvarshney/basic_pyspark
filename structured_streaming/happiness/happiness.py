import sys

from pyspark.sql import SparkSession


def main():
    if len(sys.argv) != 3:
        print("Usage: spark-submit happiness.py <host> <port>", file=sys.stderr)
        exit(-1)

    host = sys.argv[1]
    port = sys.argv[2]

    # open spark session
    spark = SparkSession\
        .builder\
        .appName("World Happiness")\
        .master("local[*]")\
        .config("spark.sql.shuffle.partitions", 3)\
        .config("spark.streaming.stopGracefullyOnShutdown","true")\
        .config("spark.sql.streaming.schemaInference", "true")\
        .getOrCreate()

    spark.sparkContext.setLogLevel('ERROR')

    readStream = spark\
        .readStream\
        .format('socket')\
        .option('host', host)\
        .option('port', port)\
        .load()

    # split the input string to exttact the country, region and happieness score
    readStream_df = readStream.selectExpr("split(value, ',')[0] as Country",\
                                 "split(value, ',')[1] as Region",\
                                 "split(value, ',')[2] as HappinessScore"\
        )

    readStream_df.createOrReplaceTempView("happiness")

    sql_stmt = """
                    SELECT 
                        Region, 
                        AVG(HappinessScore) as Avg_Happiness_Score
                    FROM happiness
                    GROUP BY Region
                """

    
    averageScore = spark.sql(sql_stmt)    

    query = averageScore\
        .writeStream\
        .format('console')\
        .outputMode('complete')\
        .start()\
        .awaitTermination()

if __name__ == "__main__":
    main()