import os

from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, to_json, col, lit, struct
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, TimestampType
 
topic_in = 'danilal146_in'
topic_out = 'danilal146_out'
app_name = 'Project_Sprint_8'
 
kafka_security_options = {
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'SCRAM-SHA-512',
    'kafka.sasl.jaas.config': 'org.apache.kafka.common.security.scram.ScramLoginModule required username="kafka-admin" password="de-kafka-admin-2022";'
}

kafka_bootstrap_servers = 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091'
 
postgresql_settings = {
    'user': 'jovyan',
    'password': 'jovyan',
    'url': 'jdbc:postgresql://localhost:5432/de',
    'driver': 'org.postgresql.Driver',
    'dbtable': 'public.subscribers_feedback',
}
 
# метод для записи данных в 2 target: в PostgreSQL для фидбэков и в Kafka для триггеров
 
def foreach_batch_function(df, epoch_id):
    # сохраняем df в памяти, чтобы не создавать df заново перед отправкой в Kafka
    df.persist()
 
    # записываем df в PostgreSQL с полем feedback
    df_to_db = df.withColumn('feedback', lit(None).cast(StringType()))

    # создаём df для отправки в Kafka. Сериализация в json.
    df_to_stream = df.select(to_json(struct(col('*'))).alias('value')).select('value')
    df_to_db.write.format('jdbc').mode('append').options(**postgresql_settings).save()

    # отправляем сообщения в результирующий топик Kafka без поля feedback
    df_to_stream.write \
        .format('kafka') \
        .option('kafka.bootstrap.servers', kafka_bootstrap_servers) \
        .options(**kafka_security_options) \
        .option('topic', topic_out) \
        .option('truncate', False) \
        .save()
 
    # очищаем память от df
    df.unpersist()

# необходимые библиотеки для интеграции Spark с Kafka и PostgreSQL
spark_jars_packages = ','.join(
    [
        'org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0',
        'org.postgresql:postgresql:42.4.0'
    ]
)
 
# создаём spark сессию с необходимыми библиотеками в spark_jars_packages для интеграции с Kafka и PostgreSQL
def spark_init(app_name):
    spark = SparkSession.builder \
        .appName(app_name) \
        .config('spark.sql.session.timeZone', 'UTC') \
        .config('spark.jars.packages', spark_jars_packages) \
        .getOrCreate() 
    return spark
 
# читаем из топика Kafka сообщения с акциями от ресторанов 
def read_stream(spark):
    df = spark.readStream \
        .format('kafka') \
        .options(**kafka_security_options) \
        .option('kafka.bootstrap.servers', 'rc1b-2erh7b35n4j4v869.mdb.yandexcloud.net:9091') \
        .option('subscribe', topic_in) \
        .load()
    # .option('kafka.ssl.truststore.location', '/usr/lib/jvm/java-1.17.0-openjdk-amd64/lib/security/cacerts') \
    # .option('kafka.ssl.truststore.password', 'changeit') 
 
    df = df.withColumn('key_str', F.col('key').cast(StringType())) \
        .withColumn('value_json', F.col('value').cast(StringType())) \
        .drop('key', 'value')
 
    # определяем схему входного сообщения для json
    incoming_message_schema = StructType([
        StructField('restaurant_id', StringType(), nullable=True),
        StructField('adv_campaign_id', StringType(), nullable=True),
        StructField('adv_campaign_content', StringType(), nullable=True),
        StructField('adv_campaign_owner', StringType(), nullable=True),
        StructField('adv_campaign_owner_contact', StringType(), nullable=True),
        StructField('adv_campaign_datetime_start', LongType(), nullable=True),
        StructField('adv_campaign_datetime_end', LongType(), nullable=True),
        StructField('datetime_created', LongType(), nullable=True),
    ])
  
    # десериализуем из value сообщения json и фильтруем по времени старта и окончания акции
    df = df.withColumn('key', F.col('key_str')) \
        .withColumn('value', F.from_json(F.col('value_json'), incoming_message_schema)) \
        .drop('key_str', 'value_json')
 
    # определяем текущее время в UTC в миллисекундах
    current_timestamp_utc = int(round(datetime.utcnow().timestamp()))

    df = df.select(
        F.col('value.restaurant_id').cast(StringType()).alias('restaurant_id'),
        F.col('value.adv_campaign_id').cast(StringType()).alias('adv_campaign_id'),
        F.col('value.adv_campaign_content').cast(StringType()).alias('adv_campaign_content'),
        F.col('value.adv_campaign_owner').cast(StringType()).alias('adv_campaign_owner'), 
        F.col('value.adv_campaign_owner_contact').cast(StringType()).alias('adv_campaign_owner_contact'), 
        F.col('value.adv_campaign_datetime_start').cast(LongType()).alias('adv_campaign_datetime_start'), 
        F.col('value.adv_campaign_datetime_end').cast(LongType()).alias('adv_campaign_datetime_end'), 
        F.col('value.datetime_created').cast(LongType()).alias('datetime_created'),
    ).filter((F.col('adv_campaign_datetime_start') <= current_timestamp_utc) & (F.col('adv_campaign_datetime_end') > current_timestamp_utc))
    return df

# вычитываем всех пользователей с подпиской на рестораны
def subscribers_restaurants(spark):
    df = spark.read \
        .format('jdbc') \
        .option('url', 'jdbc:postgresql://rc1a-fswjkpli01zafgjm.mdb.yandexcloud.net:6432/de') \
        .option('driver', 'org.postgresql.Driver') \
        .option('dbtable', 'subscribers_restaurants') \
        .option('user', 'student') \
        .option('password', 'de-student') \
        .load()
    df = df.dropDuplicates(['client_id', 'restaurant_id'])
    return df
 
# джойним данные из сообщения Kafka с пользователями подписки по restaurant_id (uuid). Добавляем время создания события.
 
def join(restaurant_read_stream_df, subscribers_restaurant_df):
    current_timestamp_utc = int(round(datetime.utcnow().timestamp()))
    df = restaurant_read_stream_df \
        .join(subscribers_restaurant_df, 'restaurant_id') \
        .withColumn('trigger_datetime_created', F.lit(current_timestamp_utc))\
        .select(
            'restaurant_id',
            'adv_campaign_id',
            'adv_campaign_content',
            'adv_campaign_owner',
            'adv_campaign_owner_contact',
            'adv_campaign_datetime_start',
            'adv_campaign_datetime_end',
            'datetime_created',
            'client_id',
            'trigger_datetime_created')
    return df

# запускаем стриминг 
 
if __name__ == '__main__':
    spark = spark_init(app_name)
    restaurant_read_stream_df = read_stream(spark)
    subscribers_restaurant_df = subscribers_restaurants(spark)
    result_df = join(restaurant_read_stream_df, subscribers_restaurant_df)
 
    result_df.writeStream \
    .foreachBatch(foreach_batch_function) \
    .start() \
    .awaitTermination() 
