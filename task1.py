#!/usr/bin/env python
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from csv import reader
import sys
from pyspark.sql import functions as F

spark = SparkSession \
     .builder \
     .appName("Task5-sql") \
     .getOrCreate()
df = spark.read.csv(sys.argv[1], sep = '\t',header = 'true')

table1 = df.replace("No Data",'null').replace('n/a','null').replace('NA','null').replace('-','null').replace('None','null')
table1.createOrReplaceTempView("table1")
nonemptycells = 0
emptycells = 0
for c in df.columns:
	print(c)	
	result = spark.sql("SELECT count(`"+c+"`) as cellcount from table1 where `"+c+ "`<> 'null' and `"+c+ "` is not null ")
	nonempty = result.collect()	
	nonemptycells += nonempty[0].cellcount
	result = spark.sql("SELECT count(`"+c+"`) as cellcount from table1 where `"+c+ "`= 'null' or `"+c+ "` is  null ")
	empty = result.collect()
	emptycells += empty[0].cellcount
	


print(nonemptycells)
print(emptycells)
#result = spark.sql("SELECT * from table1").show()
spark.stop()
