echo "Script for task 2 started"

hadoop fs -rm -r neighborhood.txt
hadoop fs -put neighborhood.txt
hadoop fs -rm -r city.txt
hadoop fs -put city.txt
hadoop fs -rm -r agency.txt
hadoop fs -put agency.txt
spark-submit --conf spark.pyspark.python=$PYSPARK_PYTHON task2.py /user/hm74/NYCColumns/6rrm-vxj9.parkname.txt.gz
echo "Script for task 2 ended"