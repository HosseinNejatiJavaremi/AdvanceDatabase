from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="CSV2Parquet")
sqlContext = SQLContext(sc)

from pyspark.sql.types import *

fields = [StructField("P_PARTKEY", IntegerType(), False),
          StructField("P_NAME", StringType(), True),
          StructField("P_MFGR", StringType(), True),
          StructField("P_BRAND", StringType(), False),
          StructField("P_TYPE", StringType(), True),
          StructField("P_SIZE", IntegerType(), True),
          StructField("P_CONTAINER", StringType(), True),
          StructField("P_RETAILPRICE", DoubleType(), True),
          StructField("P_COMMENT", StringType(), True)]

schema = StructType(fields)

rdd = sc.textFile("/data/OLAP_Benchmark_data/part.tbl")

part_df = rdd.map(lambda x: x.split("|")) \
    .map(lambda x: {'P_PARTKEY': int(x[0]),
                    'P_NAME': x[1],
                    'P_MFGR': x[2],
                    'P_BRAND': x[3],
                    'P_TYPE': x[4],
                    'P_SIZE': int(x[5]),
                    'P_CONTAINER': x[6],
                    'P_RETAILPRICE': float(x[7]),
                    'P_COMMENT': x[8]}).toDF(schema)

print(part_df.dtypes)

part_df.write.parquet("hdfs://namenode:8020/hossein-parquet-data/part.parquet",
                          mode='overwrite')

part = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/part.parquet")
print(part.dtypes)
