from pyspark import SparkContext
from pyspark.python.pyspark.shell import spark
from pyspark.sql import SQLContext

sc = SparkContext(appName="query6")
sqlContext = SQLContext(sc)

sqlContext.setConf('spark.sql.orc.impl', 'native')
lineitem = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/lineitem.orc")

from pyspark.sql import functions as F

query6 = lineitem.filter((lineitem.L_SHIPDATE >= "1997-01-01") &
                         (lineitem.L_SHIPDATE < "1998-01-01") & (lineitem.L_DISCOUNT >= 0.05)
                         & (lineitem.L_DISCOUNT <= 0.07) & (lineitem.L_QUANTITY < 24)) \
    .agg(F.sum(lineitem.L_EXTENDEDPRICE * lineitem.L_DISCOUNT)).alias('revenue')

query6.show()
