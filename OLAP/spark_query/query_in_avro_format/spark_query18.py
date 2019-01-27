from pyspark import SparkContext
from pyspark.shell import spark
from pyspark.sql import SQLContext

sc = SparkContext(appName="query18")
sqlContext = SQLContext(sc)

customer = spark.read.format("com.databricks.spark.avro")\
    .load("hdfs://namenode:8020/hossein-avro-data/customer.avro")

lineitem = spark.read.format("com.databricks.spark.avro")\
    .load("hdfs://namenode:8020/hossein-avro-data/lineitem.avro")

orders = spark.read.format("com.databricks.spark.avro")\
    .load("hdfs://namenode:8020/hossein-avro-data/orders.avro")

from pyspark.sql import functions as F

query18 = lineitem.groupBy(lineitem.L_ORDERKEY) \
    .agg(F.sum(lineitem.L_QUANTITY).alias("sum_quantity"))

query18 = query18.filter(query18.sum_quantity > 313) \
    .select(query18.L_ORDERKEY.alias("key"), query18.sum_quantity)

query18 = query18.join(orders, orders.O_ORDERKEY == query18.key) \
    .join(lineitem, orders.O_ORDERKEY == lineitem.L_ORDERKEY) \
    .join(customer, customer.C_CUSTKEY == orders.O_CUSTKEY) \
    .select(lineitem.L_QUANTITY, customer.C_NAME, customer.C_CUSTKEY,
            orders.O_ORDERKEY, orders.O_ORDERDATE, orders.O_TOTALPRICE) \
    .groupBy(customer.C_NAME, customer.C_CUSTKEY,
             orders.O_ORDERKEY, orders.O_ORDERDATE, orders.O_TOTALPRICE) \
    .agg(F.sum(lineitem.L_QUANTITY)) \
    .sort(orders.O_TOTALPRICE.desc(), orders.O_ORDERDATE) \
    .limit(100)
query18.show()
