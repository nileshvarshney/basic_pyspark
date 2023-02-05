from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import year, expr


def calculate_percentage_change(df: DataFrame) -> DataFrame:
    aggreated_df = df.withColumn("Year", year("Date")) \
        .groupBy(["Name", "Year"]) \
        .agg(expr("round(min(Low),2) as lowest"), expr("round(max(High),2) as higest")) \
        .withColumn("percent_change", expr("round((higest/lowest - 1)*100)")) \
        .sort(["Name", "Year"], ascending=False)
    return aggreated_df


def read_streaming_data(spark: SparkSession, filepath: str, max_file_per_trigger: int) -> DataFrame:
    df = spark.readStream \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .option("maxFilesPerTrigger", max_file_per_trigger) \
        .csv(filepath)
    return df
