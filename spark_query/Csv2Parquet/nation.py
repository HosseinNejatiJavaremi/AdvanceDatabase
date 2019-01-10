from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="CSV2Parquet")
sqlContext = SQLContext(sc)

from pyspark.sql.types import *

fields = [StructField("N_NATIONKEY", IntegerType(), False),
          StructField("N_NAME", StringType(), True),
          StructField("N_REGIONKEY", IntegerType(), False),
          StructField("N_COMMENT", StringType(), True)]
schema = StructType(fields)

rdd = sc.textFile("/data/OLAP_Benchmark_data/nation.tbl")

nation_df = rdd.map(lambda x: x.split("|")) \
    .map(lambda x: {'N_NATIONKEY': int(x[0]),
                    'N_NAME': x[1],
                    'N_REGIONKEY': int(x[2]),
                    'N_COMMENT': x[3]}).toDF(schema)

print(nation_df.dtypes)
nation_df.write.parquet("hdfs://namenode:8020/hossein-parquet-data/nation.parquet")

nation = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/nation.parquet")
print(nation.dtypes)
