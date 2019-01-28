from pyspark import SparkContext
from pyspark.sql import SQLContext

sc = SparkContext(appName="query19")
sqlContext = SQLContext(sc)

sqlContext.setConf('spark.sql.orc.impl', 'native')

lineitem = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/lineitem.orc")
part = sqlContext.read.orc("hdfs://namenode:8020/hossein-orc-data/part.orc")

from pyspark.sql import functions as F

fun1 = lambda x, y: x * (1 - y)

sm = ['SM CASE', 'SM BOX', 'SM PACK', 'SM PKG']
med = ['MED BAG', 'MED BOX', 'MED PKG', 'MED PACK']
lg = ['LG CASE', 'LG BOX', 'LG PACK', 'LG PKG']

query19 = part.join(lineitem, lineitem.L_PARTKEY == part.P_PARTKEY) \
    .filter(((lineitem.L_SHIPMODE == "AIR") | (lineitem.L_SHIPMODE == "AIR REG")) &
            (lineitem.L_SHIPINSTRUCT == "DELIVER IN PERSON")) \
    .filter(((part.P_BRAND == "Brand#13") &
             (part.P_CONTAINER.isin(sm)) &
             ((lineitem.L_QUANTITY >= 7) & (lineitem.L_QUANTITY <= 17)) &
             ((part.P_SIZE >= 1) & (part.P_SIZE <= 5))) |
            ((part.P_BRAND == "Brand#33") &
             (part.P_CONTAINER.isin(med)) &
             ((lineitem.L_QUANTITY >= 13) & (lineitem.L_QUANTITY <= 23)) &
             ((part.P_SIZE >= 1) & (part.P_SIZE <= 10))) |
            ((part.P_BRAND == "Brand#49") &
             (part.P_CONTAINER.isin(lg)) &
             ((lineitem.L_QUANTITY >= 21) & (lineitem.L_QUANTITY <= 31)) &
             ((part.P_SIZE >= 1) & (part.P_SIZE <= 15)))) \
    .select(fun1(lineitem.L_EXTENDEDPRICE, lineitem.L_DISCOUNT).alias("volume"))

query19 = query19.agg(F.sum("volume"))
query19.show()
