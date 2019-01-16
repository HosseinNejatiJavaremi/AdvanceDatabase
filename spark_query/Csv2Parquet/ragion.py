from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="CSV2Parquet")
sqlContext = SQLContext(sc)

from pyspark.sql.types import *

fields = [StructField("R_REGIONKEY", IntegerType(), False),
          StructField("R_NAME", StringType(), True),
          StructField("R_COMMENT", StringType(), True)]
schema = StructType(fields)

rdd = sc.textFile("/data/OLAP_Benchmark_data/region.tbl")

region_df = rdd.map(lambda x: x.split("|")) \
    .map(lambda x: {'R_REGIONKEY': int(x[0]),
                    'R_NAME': x[1],
                    'R_COMMENT': x[2]}).toDF(schema)

print(region_df.dtypes)
region_df.write.parquet("hdfs://namenode:8020/hossein-parquet-data/region.parquet",
                          mode='overwrite')

region = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/region.parquet")
region.first()
