#!/usr/bin/env python
from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from csv import reader
import sys
from pyspark.sql import functions as F
import json 
import re


spark = SparkSession \
     .builder \
     .appName("Task5-sql") \
     .getOrCreate()
df = spark.read.csv(sys.argv[1], sep = '\t',header = 'false')

df.createOrReplaceTempView("table1")



zipRegex = re.compile(r'\d{5}$|^\d{5}-\d{4}$')
phoneNumberRegex1 = re.compile(r'([0-9]( |-)?)?(\(?[0-9]{3}\)?|[0-9]{3})( |-)?([0-9]{3}( |-)?[0-9]{4}|[a-zA-Z0-9]{7})$')
phoneNumberRegex2 =re.compile(r'\D?(\d{3})\D?\D?(\d{3})\D?(\d{4})$') 
emailRegex = re.compile(r'.+@[^\.].*\.[a-z]{2,}$')
coordinatesRegex = re.compile(r'(\()([-+]?)([\d]{1,2})(((\.)(\d+)(,)))(\s*)(([-+]?)([\d]{1,3})((\.)(\d+))?(\)))$')
websiteRegex = re.compile(r'(https?:\/\/)?(www\.)?([a-zA-Z0-9]+(-?[a-zA-Z0-9])*\.)+[\w]{2,}(\/\S*)?$')

streetRegex = re.compile(r'(\d{3,})\s?(\w{0,5})\s([a-zA-Z]{2,30})\s([a-zA-Z]{2,15})\.?\s?(\w{0,5})$')
nameRegex = re.compile(r'[a-zA-Z]+(([\'\,\.\- ][a-zA-Z ])?[a-zA-Z]*)*$')



zipList = list()
phoneNumberList = list()
emailList = list()
coordinatesList = list()
streetList = list()
websiteList = list()
nameList = list()
print("Count:",df.count())

count =0

for column in df.columns:
	column_data = spark.sql("SELECT `"+column+"` as attr from table1 ").collect()
	if count ==0:
		count +=1
		boroughCount = spark.sql("select * from table1 where lower(`"+ column +"`) in ('brooklyn','bronx', 'manhattan', 'queens', 'staten island')").count()
		for row in column_data:
			if(row.attr is not None):
			
				if(re.match(zipRegex, row.attr)):
					zipList.append(row.attr)
				elif(re.match(phoneNumberRegex1, row.attr) or re.match(phoneNumberRegex2, row.attr)):
					phoneNumberList.append(row.attr)
				elif(re.match(emailRegex,row.attr)):
					emailList.append(row.attr)
				elif(re.match(coordinatesRegex, row.attr)):
					coordinatesList.append(row.attr)
				elif(re.match(streetRegex,row.attr)):
					streetList.append(row.attr)
				elif(re.match(nameRegex,row.attr)):
					nameList.append(row.attr)
				elif(re.match(websiteRegex,row.attr)):
					websitelList.append(row.attr)
		
	
				
			
print(len(zipList))
print(len(phoneNumberList))	
print(len(emailList))
print(len(coordinatesList))
print(len(streetList))
print(len(websiteList))
print(len(nameList))
print(boroughCount)
