import pytest
from pyspark.sql import SparkSession, Row
from utils.transformation import calculate_percentage_change


def test_calculate_percentage_change(source_data_df):
    output_df = calculate_percentage_change(source_data_df)
    expected = [Row(Name='AAPL', Year=2017, lowest=114.76, higest=177.2, percent_change=54.0)]
    actual = output_df.collect()
    assert expected == actual


@pytest.fixture
def source_data_df(spark):
    df = spark.read.format("csv")\
        .option("inferSchema", "true")\
        .option("header", "true")\
        .load("data/stock_data/AAPL_2017.csv")
    yield df


@pytest.fixture
def spark():
    test_spark = SparkSession.builder\
        .master("local[1]")\
        .config("spark.sql.streaming.schemaInference", "true")\
        .getOrCreate()
    yield test_spark
    test_spark.stop()
