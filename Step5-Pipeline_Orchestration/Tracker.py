import json
import logging
from datetime import datetime
from random import randint
import mysql.connector
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType

class Tracker:

    def __init__(self, jobname):
        self.jobname = jobname

    def assign_job_id(self):
        job_id = self.jobname + datetime.today().strftime("%Y%m%d")
        return job_id

    def update_job_status(self, status, assigned_job_id, connection_obj):
        job_id = self.assign_job_id()
        print("Job ID Assigned: {}".format(job_id))
        update_time = datetime.now()


        try:
            dbCursor = connection_obj.cursor()
            job_sql_command = "INSERT INTO tracker_table(id, status, time) " \
                              "VALUES('" + "1" + "', '" + status + "', '" + str(update_time) + "')"
            dbCursor.execute(job_sql_command)
            dbCursor.close()
            print("Inserted data into job tracker table.")
        except (Exception, mysql.connector.Error) as error:
            return logging.error("Error executing db statement for job tracker.", error)

    def get_db_connection(self):
        try:

            mydb = mysql.connector.connect(
                host="host",  # hostname
                user="user",  # the user who has privilege to the db
                password="password",  # password for user
                database="guidedcapstone",  # database name
                auth_plugin='mysql_native_password',

            )

            return mydb
        except Exception as error:
            logging.error("Error while connecting to database for job tracker", error)

    def data_ingestion(self):

        def parse_csv(line: str):
            record_type_pos = 2
            record = line.split(',')
            try:
                if record[record_type_pos] == 'T':
                    partition = 'T'
                    trade_dt = datetime.strptime(record[1], '%Y-%m-%d %H:%M:%S.%f').date()
                    rec_type = record[2]
                    symbol = record[3]
                    event_time = datetime.timestamp(datetime.strptime(record[4], '%Y-%m-%d %H:%M:%S.%f'))
                    event_seq_num = int(record[5])
                    exchange = record[6]
                    trade_price = float(record[7])
                    trade_size = int(record[8])
                    bid_price = None
                    bid_size = None
                    ask_price = None
                    ask_size = None

                    event = (partition, trade_dt, rec_type, symbol, event_time, event_seq_num, exchange,
                             trade_price, trade_size, bid_price, bid_size, ask_price, ask_size,
                             )
                    print("T", event)
                    return event
                elif record[record_type_pos] == 'Q':
                    partition = 'Q'
                    trade_dt = datetime.strptime(record[1], '%Y-%m-%d %H:%M:%S.%f').date()
                    rec_type = record[2]
                    symbol = record[3]
                    event_time = datetime.timestamp(datetime.strptime(record[4], '%Y-%m-%d %H:%M:%S.%f'))
                    event_seq_num = int(record[5])
                    exchange = record[6]
                    trade_price = None
                    trade_size = None
                    bid_price = float(record[7])
                    bid_size = int(record[8])
                    ask_price = float(record[9])
                    ask_size = int(record[10])

                    event = (partition, trade_dt, rec_type, symbol, event_time, event_seq_num, exchange,
                             trade_price, trade_size, bid_price, bid_size, ask_price, ask_size,
                             )

                    print("Q", event)
                    return event
                else:
                    raise Exception
            except Exception as e:
                partition = "B"
                trade_dt = None
                rec_type = None
                symbol = None
                event_time = None
                event_seq_num = None
                exchange = None
                trade_price = None
                trade_size = None
                bid_price = None
                bid_size = None
                ask_price = None
                ask_size = None
                event = (partition, trade_dt, rec_type, symbol, event_time, event_seq_num, exchange,
                         trade_price, trade_size, bid_price, bid_size, ask_price, ask_size,
                         )
                return event


        def parse_json(line: str):
            record_type_pos = 2
            record = json.loads(line)

            try:

                if record['event_type'] == 'T':

                    partition = 'T'
                    trade_dt = datetime.strptime(record['trade_dt'], '%Y-%m-%d').date()
                    rec_type = record['event_type']
                    symbol = record['symbol']
                    event_time = datetime.timestamp(datetime.strptime(record['event_tm'], '%Y-%m-%d %H:%M:%S.%f'))
                    event_seq_num = int(record['event_seq_nb'])
                    exchange = record['exchange']
                    trade_price = float(record['price'])
                    trade_size = int(record['size'])
                    bid_price = None
                    bid_size = None
                    ask_price = None
                    ask_size = None

                    event = (partition, trade_dt, rec_type, symbol, event_time, event_seq_num, exchange,
                             trade_price, trade_size, bid_price, bid_size, ask_price, ask_size
                             )
                    print(event)
                    return event
                elif record['event_type'] == 'Q':
                    partition = 'Q'
                    trade_dt = datetime.strptime(record['trade_dt'], '%Y-%m-%d').date()
                    rec_type = record['event_type']
                    symbol = record['symbol']
                    event_time = datetime.timestamp(datetime.strptime(record['event_tm'], '%Y-%m-%d %H:%M:%S.%f'))
                    event_seq_num = int(record['event_seq_nb'])
                    exchange = record['exchange']
                    trade_price = None
                    trade_size = None
                    bid_price = float(record['bid_pr'])
                    bid_size = int(record['bid_size'])
                    ask_price = float(record['ask_pr'])
                    ask_size = int(record['ask_size'])

                    event = (partition, trade_dt, rec_type, symbol, event_time, event_seq_num, exchange,
                             trade_price, trade_size, bid_price, bid_size, ask_price, ask_size
                             )

                    print(event)
                    return event
                else:
                    raise Exception
            except Exception as e:
                partition = "B"
                trade_dt = None
                rec_type = None
                symbol = None
                event_time = None
                event_seq_num = None
                exchange = None
                trade_price = None
                trade_size = None
                bid_price = None
                bid_size = None
                ask_price = None
                ask_size = None
                event = (partition, trade_dt, rec_type, symbol, event_time, event_seq_num, exchange,
                         trade_price, trade_size, bid_price, bid_size, ask_price, ask_size,
                         )

                return event

        schema = pyspark.sql.types.StructType([
            pyspark.sql.types.StructField("partition", pyspark.sql.types.StringType()),
            pyspark.sql.types.StructField("trade_dt", pyspark.sql.types.DateType()),  # DateType
            pyspark.sql.types.StructField("rec_type", pyspark.sql.types.StringType()),
            pyspark.sql.types.StructField("symbol", pyspark.sql.types.StringType()),
            pyspark.sql.types.StructField("event_tm", pyspark.sql.types.FloatType()),  # TimeStamptype
            pyspark.sql.types.StructField("event_seq_nb", pyspark.sql.types.IntegerType()),
            pyspark.sql.types.StructField("exchange", pyspark.sql.types.StringType()),
            pyspark.sql.types.StructField("trade_price", pyspark.sql.types.FloatType()),
            pyspark.sql.types.StructField("trade_size", pyspark.sql.types.IntegerType()),
            pyspark.sql.types.StructField("bid_price", pyspark.sql.types.FloatType()),
            pyspark.sql.types.StructField("bid_size", pyspark.sql.types.IntegerType()),
            pyspark.sql.types.StructField("ask_price", pyspark.sql.types.FloatType()),
            pyspark.sql.types.StructField("ask_size", pyspark.sql.types.IntegerType())
        ])

        spark = pyspark.sql.SparkSession.builder.master('local').appName('app').getOrCreate()
        # sc = SparkContext()

        spark.conf.set("adf_cloud_storage_path")
        raw = spark.sparkContext.textFile(
            "wasbs://stockstoragecontainer@aswastockstorageaccount.blob.core.windows.net/csv/2020-08-06/NYSE")
        parsed = raw.map(lambda line: parse_csv(line))

        data = spark.createDataFrame(data=parsed, schema=schema)

        data.write.partitionBy("partition").mode("overwrite").parquet("output_dir")