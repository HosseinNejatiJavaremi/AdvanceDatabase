from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="CSV2Parquet")
sqlContext = SQLContext(sc)

from pyspark.sql.types import *
fields = [StructField("C_CUSTKEY", IntegerType(), False),
          StructField("C_NAME", StringType(), True),
          StructField("C_ADDRESS", StringType(), True),
          StructField("C_NATIONKEY", IntegerType(), False),
          StructField("C_PHONE", StringType(), True),
          StructField("C_ACCTBAL", DoubleType(), True),
          StructField("C_MKTSEGMENT", StringType(), True),
          StructField("C_COMMENT", StringType(), True)]

schema = StructType(fields)

rdd = sc.textFile("/data/OLAP_Benchmark_data/customer.tbl")

customer_df = rdd.map(lambda x: x.split("|")) \
    .map(lambda x: {'C_CUSTKEY': int(x[0]),
                    'C_NAME': x[1],
                    'C_ADDRESS': x[2],
                    'C_NATIONKEY': int(x[3]),
                    'C_PHONE': x[4],
                    'C_ACCTBAL': float(x[5]),
                    'C_MKTSEGMENT': x[6],
                    'C_COMMENT': x[7]}).toDF(schema)

print(customer_df.dtypes)

customer_df.write.parquet("hdfs://namenode:8020/hossein-parquet-data/customer.parquet")

customer = sqlContext.read.parquet("hdfs://namenode:8020/hossein-parquet-data/customer.parquet")
print(customer.dtypes)
