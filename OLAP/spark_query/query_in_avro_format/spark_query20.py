from pyspark import SparkContext
from pyspark.shell import spark
from pyspark.sql import SQLContext

sc = SparkContext(appName="query20")
sqlContext = SQLContext(sc)

customer = spark.read.format("com.databricks.spark.avro")\
    .load("hdfs://namenode:8020/hossein-avro-data/customer.avro")

lineitem = spark.read.format("com.databricks.spark.avro")\
    .load("hdfs://namenode:8020/hossein-avro-data/lineitem.avro")

nation = spark.read.format("com.databricks.spark.avro")\
    .load("hdfs://namenode:8020/hossein-avro-data/nation.avro")

orders = spark.read.format("com.databricks.spark.avro")\
    .load("hdfs://namenode:8020/hossein-avro-data/orders.avro")

part = spark.read.format("com.databricks.spark.avro")\
    .load("hdfs://namenode:8020/hossein-avro-data/part.avro")

partsupp = spark.read.format("com.databricks.spark.avro") \
    .load("hdfs://namenode:8020/hossein-avro-data/partsupp.avro")

region = spark.read.format("com.databricks.spark.avro")\
    .load("hdfs://namenode:8020/hossein-avro-data/region.avro")

supplier = spark.read.format("com.databricks.spark.avro") \
    .load("hdfs://namenode:8020/hossein-avro-data/supplier.avro")


from pyspark.sql import functions as F

lineitem_filter = lineitem.filter((lineitem.L_SHIPDATE >= "1997-01-01") &
                                  (lineitem.L_SHIPDATE < "1998-01-01")) \
    .groupBy(lineitem.L_PARTKEY, lineitem.L_SUPPKEY) \
    .agg((F.sum(lineitem.L_QUANTITY) * 0.5).alias("sum_quantity"))

iran = nation.filter(nation.N_NAME == "IRAN")

iran_supplier = supplier.select(supplier.S_SUPPKEY, supplier.S_NAME,
                                supplier.S_NATIONKEY, supplier.S_ADDRESS) \
    .join(iran, supplier.S_NATIONKEY == iran.N_NATIONKEY)

query20 = part.filter(part.P_NAME.like("blue%")) \
    .select(part.P_PARTKEY).distinct() \
    .join(partsupp, part.P_PARTKEY == partsupp.PS_PARTKEY) \
    .join(lineitem_filter, (partsupp.PS_SUPPKEY == lineitem_filter.L_SUPPKEY) &
          (partsupp.PS_PARTKEY == lineitem_filter.L_PARTKEY)) \
    .filter(partsupp.PS_AVAILQTY > lineitem_filter.sum_quantity) \
    .select(partsupp.PS_SUPPKEY).distinct() \
    .join(iran_supplier, partsupp.PS_SUPPKEY == iran_supplier.S_SUPPKEY) \
    .select(iran_supplier.S_NAME, iran_supplier.S_ADDRESS) \
    .sort(iran_supplier.S_NAME)

query20.show()
