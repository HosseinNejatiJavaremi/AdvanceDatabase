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

supplier_filter = supplier.select(supplier.S_SUPPKEY, supplier.S_NATIONKEY, supplier.S_NAME)

lineitem_filter = lineitem.select(lineitem.L_SUPPKEY, lineitem.L_ORDERKEY,
                                  lineitem.L_RECEIPTDATE, lineitem.L_COMMITDATE)

lineitem_filter_time = lineitem_filter.filter(lineitem.L_RECEIPTDATE > lineitem.L_COMMITDATE)

lineitem_temp1 = lineitem_filter.groupBy(lineitem_filter.L_ORDERKEY) \
    .agg(F.countDistinct(lineitem_filter.L_SUPPKEY).alias("suppkey_count"),
         F.max(lineitem_filter.L_SUPPKEY).alias("suppkey_max"))

lineitem1 = lineitem_temp1.select(lineitem_temp1.L_ORDERKEY.alias("key"),
                                  lineitem_temp1.suppkey_count, lineitem_temp1.suppkey_max)

lineitem_temp2 = lineitem_filter_time.groupBy(lineitem_filter_time.L_ORDERKEY) \
    .agg(F.countDistinct(lineitem_filter_time.L_SUPPKEY).alias("suppkey_count"),
         F.max(lineitem_filter_time.L_SUPPKEY).alias("suppkey_max"))

lineitem2 = lineitem_temp2.select(lineitem_temp2.L_ORDERKEY.alias("key"),
                                  lineitem_temp2.suppkey_count, lineitem_temp2.suppkey_max)

orders_filter = orders.select(orders.O_ORDERKEY, orders.O_ORDERSTATUS) \
    .filter(orders.O_ORDERSTATUS == "F")

query21 = nation.filter(nation.N_NAME == "IRAN") \
    .join(supplier_filter, nation.N_NATIONKEY == supplier_filter.S_NATIONKEY) \
    .join(lineitem_filter_time, supplier_filter.S_SUPPKEY == lineitem_filter_time.L_SUPPKEY) \
    .join(orders_filter, lineitem_filter_time.L_ORDERKEY == orders_filter.O_ORDERKEY) \
    .join(lineitem1, lineitem_filter_time.L_ORDERKEY == lineitem1.key) \
    .filter((lineitem1.suppkey_count > 1) |
            ((lineitem1.suppkey_count == 1) & (lineitem_filter_time.L_SUPPKEY == lineitem1.suppkey_max)))

query21 = query21.select(query21.S_NAME, query21.L_ORDERKEY, query21.L_SUPPKEY) \
    .join(lineitem2, query21.L_ORDERKEY == lineitem2.key, "left_outer") \
    .select(query21.S_NAME, query21.L_ORDERKEY, query21.L_SUPPKEY,
            lineitem2.suppkey_count, lineitem2.suppkey_max) \
    .filter((lineitem2.suppkey_count == 1) & (query21.L_SUPPKEY == lineitem2.suppkey_max)) \
    .groupBy(query21.S_NAME) \
    .agg(F.count(query21.L_SUPPKEY).alias("numwait"))

query21 = query21.sort(query21.numwait.desc(), query21.S_NAME) \
    .limit(100)
query21.show()
