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

        conf = SparkConf().setAppName(f"Recommendation_mart-{date}")
        sc = SparkContext(conf=conf)
        sql = SQLContext(sc)

        dt = datetime.datetime.strptime(date, '%Y-%m-%d')

        recommendation_mart = recommendation_mart(date, days_count, sql)

        recommendation_mart.write.parquet(f"{base_output_path}/recommendation_mart")

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

def recommendation_mart(date, depth, spark):
    paths = input_event_paths(date, depth)
    ev = spark.read.parquet(*paths)
    
    m = ev.where('event_type = "message"')\
      .where('event.message_channel_to is not null')\
      .selectExpr('event.message_from as user_id', 'event.datetime', 'to_date(event.datetime) as date',
                  'lat', 'lon', f'lat*{math.pi}/180 as lat_r', f'lon*{math.pi}/180 as lon_r')
    
    c = ev.where('event_type = "message"')\
      .selectExpr('event.message_from as user_id', 'event.message_to as user_id_to', 'event.datetime',
                  'to_date(event.datetime) as date', 'lat', 'lon', f'lat*{math.pi}/180 as lat_r', f'lon*{math.pi}/180 as lon_r')
    
    s = ev.where('event_type = "subscription"').where('lat is not null')\
      .selectExpr('event.user as user_id', 'event.subscription_channel as channel', 'event.datetime', 
                  'to_date(event.datetime) as date', 'lat', 'lon', f'lat*{math.pi}/180 as lat_r', f'lon*{math.pi}/180 as lon_r')
 
    mx = geo_data(m)
    cx = geo_data(c)
    sx = geo_data(s)
    
    user1 = sx.select('user_id', 'channel')
    user2 = sx.selectExpr('user_id as user_id_channel', 'channel')
    channel_users = user1.join(user2, on='channel').where('user_id <> user_id_channel')
    
    users = channel_users.join(cx, on='user_id').where('user_id_to <> user_id_channel')\
                      .dropDuplicates(['user_id', 'user_id_channel'])\
                      .select('user_id', 'user_id_channel', 'lat_r', 'lon_r', 'id')
    
    users = users.join(mx.selectExpr('user_id as user_id_channel', 'lat_r as lat_r_to', 'lon_r as lon_r_to'),
                       on = 'user_id_channel', how='inner')\
                 .withColumn('dif',
                             F.acos(F.sqrt(
                                   (F.lit(2*6371)*F.pow(F.sin((F.col('lat_r') - F.col('lat_r_to'))/F.lit(2)), 2)+\
                                   (F.cos('lat_r')*F.cos('lat_r_to')*F.pow(F.sin((F.col('lon_r') - F.col('lon_r_to'))/F.lit(2)), 2)
                                   )
                                   ))))\
                 .filter(F.col('dif')<=1)\
            .selectExpr('user_id as user_left', 'user_id_channel as user_right', f'"{date}" as processed_dttm', 'id as zone_id')
    
    user_mart = spark.read.parquet('hdfs://rc1a-dataproc-m-dg5lgqqm7jju58f9.mdb.yandexcloud.net:8020/user/danilal146/data/prod/user_mart')
    
    recommendation_mart = users.join(user_mart, users.user_left == user_mart.user_id, how='inner')\
                               .select('user_left', 'user_right', 'processed_dttm', 'zone_id', 'local_time').show()
    
    return recommendation_mart

if __name__ == "__main__":
        main()