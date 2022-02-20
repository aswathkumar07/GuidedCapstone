from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DateType, FloatType
from datetime import datetime
import json

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

def parse_json(line: str) :
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


schema = StructType([
    StructField("partition",StringType()),
    StructField("trade_dt",DateType()), #DateType
    StructField("rec_type",StringType()),
    StructField("symbol",StringType()),
    StructField("event_tm", FloatType()), #TimeStamptype
    StructField("event_seq_nb", IntegerType()),
    StructField("exchange", StringType()),
    StructField("trade_price", FloatType()),
    StructField("trade_size", IntegerType()),
    StructField("bid_price", FloatType()),
    StructField("bid_size", IntegerType()),
    StructField("ask_price", FloatType()),
    StructField("ask_size", IntegerType())
  ])

spark = SparkSession.builder.master('local').appName('app').getOrCreate()

spark.conf.set("fs.azure.account.key.aswastockstorageaccount.blob.core.windows.net","G/0A9QEFualVTCqoaASkYPjQIllBRrCEsJIamplgTxVcsK4wwkzV1zBUuyKyQTLBkfGewtlQAi/pcKsb+TJ0ZQ==")

raw_csv = spark.sparkContext.textFile("wasbs://stockstoragecontainer@aswastockstorageaccount.blob.core.windows.net/csv/*/NYSE")

parsed_csv = raw_csv.map(lambda line: parse_csv(line))

data_csv = spark.createDataFrame(data=parsed_csv, schema=schema)

data_csv.write.partitionBy("partition").mode("overwrite").parquet("output_dir")

