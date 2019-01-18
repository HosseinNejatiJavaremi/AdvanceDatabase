from pyspark import SparkContext
from pyspark.python.pyspark.shell import spark
from pyspark.sql import SQLContext

sc = SparkContext(appName="query2")
sqlContext = SQLContext(sc)

sqlContext.setConf('spark.sql.orc.impl', 'native')
nation = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/nation.orc")
part = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/part.orc")
partsupp = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/partsupp.orc")
region = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/region.orc")
supplier = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/supplier.orc")

ASIA = region.filter(region.R_NAME == 'ASIA') \
    .join(nation, region.R_REGIONKEY == nation.N_REGIONKEY) \
    .join(supplier, nation.N_NATIONKEY == supplier.S_NATIONKEY) \
    .join(partsupp, supplier.S_SUPPKEY == partsupp.PS_SUPPKEY)

COPPER = part.filter((part.P_SIZE == 50) & (part.P_TYPE.like("%COPPER"))) \
    .join(ASIA, ASIA.PS_PARTKEY == part.P_PARTKEY)

from pyspark.sql import functions as F

min_cost = COPPER.groupBy(COPPER.PS_PARTKEY) \
    .agg(F.min(COPPER.PS_SUPPLYCOST).alias("min"))

query2 = COPPER.join(min_cost, COPPER.PS_PARTKEY == min_cost.PS_PARTKEY) \
    .filter(COPPER.PS_SUPPLYCOST == min_cost.min) \
    .select("S_ACCTBAL", "S_NAME", "N_NAME", COPPER.P_PARTKEY, "P_MFGR",
            "S_ADDRESS", "S_PHONE", "S_COMMENT") \
    .sort(COPPER.S_ACCTBAL.desc(), "N_NAME", "S_NAME", COPPER.P_PARTKEY) \
    .limit(100)
query2.show()
