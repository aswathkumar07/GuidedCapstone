import sys
import logging
from Reporter import Reporter
from pyspark.sql import SparkSession

# Create SparkSession object
spark = SparkSession.builder.master('local').appName('app').getOrCreate()
sc = spark.sparkContext

eod_dir = r"C:/Spark/spark3/output_dir"


# Main Reporter Python script
def run_reporter_etl():
    trade_date = '2020-07-29'
    reporter = Reporter(spark)
    try:
        reporter.report(spark, trade_date, eod_dir)
    except Exception as e:
        print(e)
    return


# Call above functions using arguments created and specified below
if __name__ == "__main__":

    run_reporter_etl()