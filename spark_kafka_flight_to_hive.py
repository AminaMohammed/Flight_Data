import sys

from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import json
import re

# reload(sys)
#sys.setdefaultencoding('utf-8')

sc = SparkContext(appName="SparkStreaming")
sqlContext = SQLContext(sc)
sc.setLogLevel("WARN")
ssc = StreamingContext(sc, 5) # 5 seconds window

spark = SparkSession.builder\
        .appName("SparkToHiveStreaming")\
        .config("spark.sql.warehouse.dir","/usr/hive/warehouse")\
        .config("spark.sql.catalogImplementation","hive")\
        .config("hive.metastore.uris","thrift://localhost:9083")\
        .enableHiveSupport()\
        .getOrCreate()

spark.sql("DROP TABLE IF EXISTS Flight_data_all")

kvs = KafkaUtils.createDirectStream(ssc, ["flight-topic"],
                          {"metadata.broker.list":"localhost:9096"})

data = kvs.map(lambda x: x[1])

cnt = 0

def readRdd4rmKafkaStream(readRDD):
    global cnt
    if not readRDD.isEmpty():
        # Put RDD into a dataframe
        df = sqlContext.read.json(readRDD)
        #df.registerTempTable("flightdata")
        #  Filter the columns
        df_cols = df.columns
        desired_cols = ['city_name','code','country_code','display_name',
        'display_sub_title','display_title','latitude','location_id','longitude','name','state','time_zone_name']
        try:
            df=df.select(desired_cols)
            df.show()
            if cnt < 1:
                df.write.mode("overwrite")\
                    .saveAsTable("flight_db.Flight_data_all")
                cnt+=1
            else:
                df.write.mode("append")\
                    .saveAsTable("flight_db.Flight_data_all")

        except Exception:
            print('pass')
            pass

data.foreachRDD(lambda rdd: readRdd4rmKafkaStream(rdd))
print("\n\n\n\n\n\n\nHEY, CAN YOU SEE ME\n\n\n\n\n\n\n")
ssc.start()
ssc.awaitTermination()


# spark-submit --packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.4.4 spark_kafka_flight_to_hive.py
