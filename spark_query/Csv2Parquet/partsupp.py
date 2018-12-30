from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="CSV2Parquet")
sqlContext = SQLContext(sc)

from pyspark.sql.types import *

fields = [StructField("PS_PARTKEY", IntegerType(), False),
          StructField("PS_SUPPKEY", IntegerType(), False),
          StructField("PS_AVAILQTY", IntegerType(), True),
          StructField("PS_SUPPLYCOST", DoubleType(), True),
          StructField("PS_COMMENT", StringType(), True)]

schema = StructType(fields)

rdd = sc.textFile("/data/OLAP_Benchmark_data/partsupp.tbl")

partsupp_df = rdd.map(lambda x: x.split("|")) \
    .map(lambda x: {'PS_PARTKEY': int(x[0]),
                    'PS_SUPPKEY': int(x[1]),
                    'PS_AVAILQTY': int(x[2]),
                    'PS_SUPPLYCOST': float(x[3]),
                    'PS_COMMENT': x[4]}).toDF(schema)

print(partsupp_df.dtypes)

partsupp_df.write.parquet("hdfs://namenode:8020/hossein-parquet-data/partsupp.parquet")

partsupp = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/partsupp.parquet")
print(partsupp.dtypes)
