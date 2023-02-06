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

        conf = SparkConf().setAppName(f"User_mart-{date}")
        sc = SparkContext(conf=conf)
        sql = SQLContext(sc)

        dt = datetime.datetime.strptime(date, '%Y-%m-%d')

        user_mart = user_mart(date, days_count, sql)

        user_mart.write.parquet(f"{base_output_path}/user_mart")

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

def load_geo_timezone():
    ### Справочник городов и таймзон
    ### Не все таймзоны определяются через Australia/City. Для городов, которые не определяются, ищу ближайщие города из справочника.
    geo = load_geo_cities('')
    geo = geo.selectExpr('id','city', f'lat*{math.pi}/180 as lat_geo', f'lng*{math.pi}/180 as lon_geo')
        
    # Датафрейм с городами, для которых определяются таймзоны
    geo_r = geo.selectExpr('id as id_r','city as city_r', 'lat_geo as lat_r', 'lon_geo as lon_r')\
           .where('city in ("Brisbane","Sydney","Adelaide","Perth","Melbourne","Darwin","Hobart","Canberra")')
        
    # Датафрейм с остальными городами
    geo_w = geo.where('city not in ("Brisbane","Sydney","Adelaide","Perth","Melbourne","Darwin","Hobart","Canberra")')

    # Для остальных городов ищу ближайший город из тех, для которых определяются таймзоны    
    geo_x = geo_r.join(geo_w, how='cross')
    geo_x = geo_x.select('id', 'city_r', 'city', F.acos(F.sqrt(
    (F.pow(F.sin((F.col('lat_r') - F.col('lat_geo'))/F.lit(2)), 2)+\
    (F.cos('lat_r')*F.cos('lat_geo')*F.pow(F.sin((F.col('lon_r') - F.col('lon_geo'))/F.lit(2)), 2)
    )
    ))).alias("len"))\
    .withColumn("rank", F.row_number().over(Window.partitionBy("city").orderBy(F.asc("len")))).where('rank=1')\
    .withColumn("timezone",F.concat(F.lit("Australia/"),F.col('city_r')))\
    .select('id', 'city', 'timezone')

    # Итоговый датафрейм со всеми городами и таймзонами
    geo_timezone = geo_r.selectExpr('id_r as id', 'city_r as city')\
    .withColumn("timezone",F.concat(F.lit("Australia/"),F.col('city')))\
    .union(geo_x)
    return geo_timezone

def user_mart(date, depth, spark):
    message_paths = input_event_paths(date, depth)
    df = spark.read.parquet(*message_paths)
    geo = load_geo_cities('')
    
    df = df.where('event_type = "message"')\
      .where('event.message_channel_to is not null')\
      .selectExpr('event.message_from as user_id', 'event.datetime', 'to_date(event.datetime) as date', 'lat', 'lon',
                   f'lat*{math.pi}/180 as lat_r', f'lon*{math.pi}/180 as lon_r')
    
    df = df.join(geo, how='cross')
    df = df.select('date', 'datetime', 'user_id', 'lat', 'lon', 'city', F.acos(F.sqrt(
    (F.pow(F.sin((F.col('lat_r') - F.col('lat_geo'))/F.lit(2)), 2)+\
    (F.cos('lat_r')*F.cos('lat_geo')*F.pow(F.sin((F.col('lon_r') - F.col('lon_geo'))/F.lit(2)), 2)
    )
    ))).alias("len"))
    df = df.withColumn("rank", F.row_number().over(Window.partitionBy("datetime", "user_id").orderBy(F.asc("len")))).where('rank=1')
    
    act_city = df.select('date', 'user_id', 'city').withColumn("rank_city",       F.row_number().over(Window.partitionBy("user_id").orderBy(F.desc("date")))).where('rank_city=1')\
    .selectExpr('user_id', 'city as actual_city')
    
    windowSpec = Window.partitionBy("user_id", "city").orderBy("date")

    home_city = df.withColumn("max_date", F.max('date').over(windowSpec)).withColumn("row_number", F.row_number().over(windowSpec))\
    .withColumn('delta', F.datediff(F.lit(date),F.col('max_date'))).where('delta >= 27')\
    .withColumn("rank", F.row_number().over(Window.partitionBy("user_id").orderBy(F.desc("datetime")))).where('rank = 1')\
    .selectExpr('user_id', 'city as home_city')
    
    window_lag = Window().partitionBy(F.col("user_id"), F.col("city")).orderBy(F.col("date"))

    travel_array = msg.select('datetime', 'date', 'user_id', 'city')\
    .withColumn("lag_city", F.lag("city", 1, 0).over(window_lag)).where('lag_city = 0')\
    .groupBy('user_id').agg(F.concat_ws(", ", F.collect_list('city')).alias('travel_array'))
    
    travel_count = msg.select('datetime', 'date', 'user_id', 'city')\
    .withColumn("lag_city", F.lag("city", 1, 0).over(window_lag)).where('lag_city = 0')\
    .groupBy('user_id').agg(F.expr("count(city) as travel_count"))

    last_msg = df.withColumn("rank_time", F.row_number().over(Window.partitionBy("user_id")\
                 .orderBy(F.desc("datetime")))).where('rank_time=1')\
                 .selectExpr('user_id', 'city', 'datetime')
    
    local_time = last_msg.join(load_geo_timezone(), on=['city'], how='left')\
                         .withColumn("TIME_UTC",F.col("datetime").cast("Timestamp"))\
                         .withColumn('local_time', F.from_utc_timestamp(F.col("TIME_UTC"), F.col('timezone')))
    
    user_mart = act_city.join(home_city, on='user_id', how='left')\
                        .join(travel_count, on='user_id', how='left')\
                        .join(travel_array, on='user_id', how='left')\
                        .join(local_time, on='user_id', how='left')\
                        .withColumn('home_city_out', F.coalesce(F.col('home_city'), F.col('actual_city')))\
    .selectExpr('user_id', 'actual_city', 'home_city_out as home_city', 'travel_count', 'travel_array', 'local_time')
    
    return user_mart

if __name__ == "__main__":
        main()
