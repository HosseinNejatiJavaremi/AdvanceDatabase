from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="CSV2Parquet")
sqlContext = SQLContext(sc)

from pyspark.sql.types import *

fields = [StructField("O_ORDERKEY", IntegerType(), False),
          StructField("O_CUSTKEY", IntegerType(), False),
          StructField("O_ORDERSTATUS", StringType(), False),
          StructField("O_TOTALPRICE", DoubleType(), True),
          StructField("O_ORDERDATE", StringType(), True),
          StructField("O_ORDERPRIORITY", StringType(), True),
          StructField("O_CLERK", StringType(), True),
          StructField("O_SHIPPRIORITY", IntegerType(), True),
          StructField("O_COMMENT", StringType(), True)]

schema = StructType(fields)

rdd = sc.textFile("/data/OLAP_Benchmark_data/orders.tbl")

orders_df = rdd.map(lambda x: x.split("|")) \
    .map(lambda x: {'O_ORDERKEY': int(x[0]),
                    'O_CUSTKEY': int(x[1]),
                    'O_ORDERSTATUS': x[2],
                    'O_TOTALPRICE': float(x[3]),
                    'O_ORDERDATE': x[4],
                    'O_ORDERPRIORITY': x[5],
                    'O_CLERK': x[6],
                    'O_SHIPPRIORITY': int(x[7]),
                    'O_COMMENT': x[8]}).toDF(schema)

print(orders_df.dtypes)

orders_df.write.parquet("hdfs://namenode:8020/hossein-parquet-data/orders.parquet")

orders = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/orders.parquet")
print(orders.dtypes)
