import datetime
import os
from datetime import datetime, timedelta
from pyspark.sql import SparkSession



class Reporter(object):
    def __init__(self, spark):
        self.spark = spark
    def report(self, spark, trade_date, eod_dir):
        date = datetime.strptime(trade_date, "%Y-%m-%d")
        prev_date = date - timedelta(days=1)
        prev_date_str = prev_date.strftime("%Y-%m-%d")

        # Trade

        spark = SparkSession.builder.master('local').appName('app').getOrCreate()
        spark.conf.set("adf_storage_account_key")

        trades_df = spark.read.parquet("path/trade/trade_dt={}".format(trade_date))
        trades_df.createOrReplaceTempView("trades")

        tmp_trade_df = spark.sql(
            "select trade_dt, symbol, exchange, event_tm, event_seq_nb, trade_price from trades where trade_dt ='{}'".format(trade_date))
        tmp_trade_df.createOrReplaceTempView("tmp_trade_moving_avg")
        tmp_trade_df.show()

        mov_avg_df = spark.sql("""
               select trade_dt, symbol, exchange, event_tm, event_seq_nb, trade_price,
               AVG(trade_price) OVER (PARTITION BY (symbol, exchange)
               ORDER BY CAST(event_tm AS timestamp)
               RANGE BETWEEN INTERVAL 30 MINUTES PRECEDING AND CURRENT ROW) as mov_avg_pr
               from tmp_trade_moving_avg""")

        mov_avg_df.write.saveAsTable("temp_trade_moving_avg")
        mov_avg_df.show()
        #
        df_prev_date = spark.sql(
            "select symbol, event_tm, event_seq_nb, trade_price from trades where trade_dt = '{}'".format(prev_date_str))

        df_prev_date.createOrReplaceTempView("tmp_last_trade")
        last_trade_df = spark.sql("""select a.trade_dt, a.symbol, a.exchange, a.event_tm,
           a.event_seq_nb, a.last_pr from
           (select trade_dt, symbol, exchange, event_tm, event_seq_nb, trade_price,
           AVG(trade_price)
           OVER (PARTITION BY (symbol)
           ORDER BY CAST(event_tm as timestamp)
           RANGE BETWEEN INTERVAL 30 MINUTES PRECEDING AND CURRENT ROW) as last_pr
           FROM tmp_trade_moving_avg) a
           """)

        last_trade_df.write.saveAsTable("temp_last_trade")

        quotes_df = spark.read.parquet("path/quote/quote_dt={}".format(trade_date))

        quotes_df.createOrReplaceTempView("quotes")

        quote_union = spark.sql("""
           SELECT trade_dt, rec_type,  symbol,event_tm,event_seq_nb,exchange,bid_price,bid_size,ask_price,
           ask_size,null as trade_price,null as mov_avg_pr FROM quotes
           UNION ALL
           SELECT trade_dt, "Q" as rec_type, symbol,event_tm,event_seq_nb,exchange,null as bid_price,null as bid_size,null as ask_price,
           null as ask_size,trade_price,mov_avg_pr FROM temp_trade_moving_avg
           """)

        quote_union.createOrReplaceTempView("quote_union")

        quote_union_update = spark.sql("""
           select *,
           last_value(trade_price, true) OVER (PARTITION BY symbol, exchange ORDER BY event_tm ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as last_trade_price,
           last_value(mov_avg_pr, true) OVER (PARTITION BY symbol, exchange ORDER BY event_tm ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as last_mov_avg_pr
           from quote_union
           """)

        quote_union_update.createOrReplaceTempView("quote_union_update")

        quote_update = spark.sql("""
               select trade_dt, symbol, event_tm, event_seq_nb, exchange,
               bid_price, bid_size, ask_price, ask_size, last_trade_price, last_mov_avg_pr
               from quote_union_update
               where rec_type = 'Q' and last_trade_price is not null
           """)

        quote_update.createOrReplaceTempView("quote_update")

        quote_final = spark.sql("""
               select trade_dt, symbol, event_tm, event_seq_nb, exchange,
               bid_price, bid_size, ask_price, ask_size, last_trade_price, last_mov_avg_pr,
               bid_price - close_price as bid_pr_mv,
               ask_price - close_price as ask_pr_mv
               from (
               select /* + BROADCAST(t) */
               q.trade_dt, q.symbol, q.event_tm, q.event_seq_nb, q.exchange, q.bid_price, q.bid_size, q.ask_price, q.ask_size, q.last_trade_price,q.last_mov_avg_pr,
               t.last_pr as close_price
               from quote_update q
               left outer join temp_last_trade t on
               (q.symbol = t.symbol and q.exchange = t.exchange))
           """)

        quote_final.show()
        quote_final.write.parquet("path_new1/trade_dt={}".format(trade_date))