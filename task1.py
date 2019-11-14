#!/usr/bin/env python
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from csv import reader
import sys
from pyspark.sql import functions as F
import json
import re
import statistics as stats
from dateutil.parser import parse


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
intRegex = re.compile(r'^[-+]?[0-9]+$')
realRegex = re.compile(r'([0-9]+(?:\.[0-9]+)?)(?:\s)')


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
	column_json["data_types"]=list()
	intList = list()
	realList = list()
	dateTimeList = list()
	textList = list()
	totalChars = 0
	column_data = spark.sql("SELECT `"+column+"` as attr from table1 ").collect()
	for row in column_data:
		if(re.match(intRegex, row.attr)):
			intList.append(int(row.attr))
			realList.append(float(row.attr))
		elif(re.match(realRegex, row.attr)):
			realList.append(row.attr)
		else:
			try:
				dateTime = parse(row.attr)
				dateTimeList.append(dateTime)
			except (ValueError, OverflowError) as e:
				totalChars += len(row.attr)
				textList.append(row.attr)

	if intList:
		intJson = {"type":"INTEGER (LONG)",
		"count":len(intList),
		"max_value":max(intList),
		"min_value":min(intList),
		"mean":stats.mean(intList),
		"stddev":stats.stdev(intList)
		}
		column_json["data_types"].append(intJson)
	if realList:
		realJson = {"type":"REAL",
		"count":len(realList),
		"max_value":max(realList),
		"min_value":min(realList),
		"mean":stats.mean(realList),
		"stddev":stats.stdev(realList)
		}
		column_json["data_types"].append(realJson)

	if dateTimeList:
		dateTimeJson = {"type":"DATE/TIME",
		"count":len(dateTimeList),
		"max_value":max(dateTimeList),
		"min_value":min(dateTimeList)
		}
		column_json["data_types"].append(dateTimeJson)

	if textList:
		textListSorted = sorted(textList, key=lambda textAttr:len(textAttr))
		textJson = {"type":"TEXT",
		"count":len(textList),
		"shortest_values":textListSorted[:5],
		"longest_values":textListSorted[-5:],
		"average_length":totalChars/len(textList)
		}
		column_json["data_types"].append(textJson)

	result_json["columns"].append(column_json)

filename=sys.argv[1].split('/')[-1]+ ".json"

with open(filename, 'w') as f:
    json.dump(result_json, f)


spark.stop()
