#!/usr/bin/env python
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from csv import reader
import sys
from pyspark.sql import functions as F
import json 

spark = SparkSession \
     .builder \
     .appName("Task5-sql") \
     .getOrCreate()
df = spark.read.csv(sys.argv[1], sep = '\t',header = 'true')

table1 = df.replace("No Data",'null').replace('n/a','null').replace('NA','null').replace('-','null').replace('None','null')
table1.createOrReplaceTempView("table1")


result_json = {
    "dataset_name": sys.argv[1].split('/')[-1]
    }
result_json["columns"]=list()

for column in df.columns:	
	nonemptycells = 0
	emptycells = 0
	result = spark.sql("SELECT COUNT(`"+column+"`) as cellcount from table1 WHERE `"+column+ "`<> 'null' and `"+column+ "` is not null ")
	nonempty = result.collect()	
	nonemptycells = nonempty[0].cellcount
	result = spark.sql("SELECT COUNT(`"+column+"`) as cellcount from table1 WHERE `"+column+ "`= 'null' or `"+column+ "` is  null ")
	empty = result.collect()
	emptycells = empty[0].cellcount
	result = spark.sql("SELECT COUNT(DISTINCT `"+column+"`) as distinct_count from table1")
	distinct_values=result.collect()[0].distinct_count
	result = spark.sql("SELECT `"+column+"` as attr, COUNT(`"+column+"`) as frequency from table1 GROUP BY `"+column+"` ORDER BY frequency DESC LIMIT 5")
	top_frequent_elements = [row.attr for row in result.collect()]

	column_json={
		"column_name": column,
		"number_non_empty_cells":nonemptycells,
		"number__empty_cells":emptycells,
		"number_distinct_values":distinct_values,
		"frequent_values":top_frequent_elements
	}
	result_json["columns"].append(column_json)

filename=sys.argv[1].split('/')[-1]+ ".json"

with open(filename, 'w') as f:
    json.dump(result_json, f)


spark.stop()
