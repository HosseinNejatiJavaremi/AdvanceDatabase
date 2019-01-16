from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="CSV2Parquet")
sqlContext = SQLContext(sc)

from pyspark.sql.types import *

fields = [StructField("S_SUPPKEY", IntegerType(), False),
          StructField("S_NAME", StringType(), True),
          StructField("S_ADDRESS", StringType(), True),
          StructField("S_NATIONKEY", IntegerType(), False),
          StructField("S_PHONE", StringType(), True),
          StructField("S_ACCTBAL", DoubleType(), True),
          StructField("S_COMMENT", StringType(), True)]

schema = StructType(fields)

rdd = sc.textFile("/data/OLAP_Benchmark_data/supplier.tbl")

supplier_df = rdd.map(lambda x: x.split("|")) \
    .map(lambda x: {'S_SUPPKEY': int(x[0]),
                    'S_NAME': x[1],
                    'S_ADDRESS': x[2],
                    'S_NATIONKEY': int(x[3]),
                    'S_PHONE': x[4],
                    'S_ACCTBAL': float(x[5]),
                    'S_COMMENT': x[6]}).toDF(schema)

print(supplier_df.dtypes)

supplier_df.write.parquet("hdfs://namenode:8020/hossein-parquet-data/supplier.parquet",
                          mode='overwrite')

supplier = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/supplier.parquet")
print(supplier.dtypes)
supplier.first()
