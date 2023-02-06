import sys
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, FloatType
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
import pyspark.sql.functions as F
from pyspark.sql.window import Window
import datetime
import math

def main():
        date = sys.argv[1] #'2022-05-31'
        days_count = sys.argv[2] #10
        base_output_path = sys.argv[3] #'hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/danilal146/data/prod'

        conf = SparkConf().setAppName(f"Geo_zone_mart-{date}")
        sc = SparkContext(conf=conf)
        sql = SQLContext(sc)

        dt = datetime.datetime.strptime(date, '%Y-%m-%d')

        geo_zone_mart = geo_zone_mart(date, days_count, sql)

        geo_zone_mart.write.parquet(f"{base_output_path}/geo_zone_mart")

def input_event_paths(date, depth):
    dt = datetime.datetime.strptime(date, '%Y-%m-%d')
    
    return [f"hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/master/data/geo/events/date={(dt-datetime.timedelta(days=x)).strftime('%Y-%m-%d')}" for x in range(depth)]

def load_geo_cities(geo_path):
    chema = StructType([
    StructField("id", IntegerType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("lat", FloatType(), nullable=True),
    StructField("lng", FloatType(), nullable=True)])

    geo = spark.read.option('header',True).option('delimiter',';').csv(geo_path + 'https://code.s3.yandex.net/data-analyst/data_engeneer/geo.csv')  
    geo = data.withColumn('lat', F.regexp_replace('lat', ',', '.')).withColumn('lng', F.regexp_replace('lng', ',', '.'))
    geo = geo.selectExpr('id','city', f'lat*{math.pi}/180 as lat_geo', f'lng*{math.pi}/180 as lon_geo')
    
    return geo

def geo_data(df):
    geo = load_geo_cities('')
    df = df.join(geo, how='cross')
    df = df.withColumn("len", F.acos(F.sqrt(
    (F.pow(F.sin((F.col('lat_r') - F.col('lat_geo'))/F.lit(2)), 2)+\
    (F.cos('lat_r')*F.cos('lat_geo')*F.pow(F.sin((F.col('lon_r') - F.col('lon_geo'))/F.lit(2)), 2)
    )))))\
    .withColumn("rank", F.row_number().over(Window.partitionBy("datetime", "user_id").orderBy(F.asc("len")))).where('rank=1')\
    .drop("lat_geo","lon_geo","lat", "lon", "len")
    
    return df

def event_count_by_city(df, event_type):
    cnt_month = df.withColumn("month",F.trunc(F.col("date"), "month"))\
    .groupby('city', 'month').agg(F.expr(f"count(*) as month_{event_type}"))

    cnt_week = df.withColumn("week",F.trunc(F.col("date"), "week"))\
    .withColumn("month",F.trunc(F.col("date"), "month"))\
    .groupby('city', 'month', 'week').agg(F.expr(f"count(*) as week_{event_type}"))
    
    return cnt_week.join(cnt_month, on=['city', 'month'], how='left')

def geo_zone_mart(date, depth, spark):
    paths = input_event_paths(date, depth)
    ev = spark.read.parquet(*paths)
    
    m = ev.where('event_type = "message"')\
      .where('event.message_channel_to is not null')\
      .selectExpr('event.message_from as user_id', 'event.datetime', 'to_date(event.datetime) as date',
                  'lat', 'lon', f'lat*{math.pi}/180 as lat_r', f'lon*{math.pi}/180 as lon_r')

    c = ev.where('event_type = "message"')\
      .selectExpr('event.message_from as user_id', 'event.message_to as user_id_to', 'event.datetime',
                  'to_date(event.datetime) as date', 'lat', 'lon', f'lat*{math.pi}/180 as lat_r', f'lon*{math.pi}/180 as lon_r')

    r = ev.where('event_type = "reaction"').where('lat is not null')\
          .selectExpr('event.reaction_from as user_id', 'event.datetime', 'to_date(event.datetime) as date',
          'lat', 'lon', f'lat*{math.pi}/180 as lat_r', f'lon*{math.pi}/180 as lon_r')

    s = ev.where('event_type = "subscription"').where('lat is not null')\
      .selectExpr('event.user as user_id', 'event.subscription_channel as channel', 'event.datetime', 
                  'to_date(event.datetime) as date', 'lat', 'lon', f'lat*{math.pi}/180 as lat_r', f'lon*{math.pi}/180 as lon_r')
    
    mx = geo_data(m)
    cx = geo_data(c)
    rx = geo_data(r)
    sx = geo_data(s)
    regx = mx.withColumn("rank", F.row_number().over(Window.partitionBy("user_id").orderBy(F.asc("datetime")))).where('rank=1')
    
    geo_zone_mart = event_count_by_city(mx, 'message').join(event_count_by_city(rx, 'reaction'), on=['city', 'month', 'week'], how='left')\
                                      .join(event_count_by_city(sx, 'subscription'), on=['city', 'month', 'week'], how='left')\
                                      .join(event_count_by_city(regx, 'registration'), on=['city', 'month', 'week'], how='left')\
                                      .join(load_geo_cities('').select('id', 'city'), on=['city'], how='left')\
            .selectExpr('month', 'week', 'id as zone_id', 'week_message', 'week_reaction', 'week_subscription',
            'week_registration as week_user', 'month_message', 'month_reaction', 'month_subscription',
            'month_registration as month_user').show(20)
    
    return geo_zone_mart

if __name__ == "__main__":
        main()