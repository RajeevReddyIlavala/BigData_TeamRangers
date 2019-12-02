echo "Script for task 2 started"

spark-submit --conf spark.pyspark.python=$PYSPARK_PYTHON task2.py /user/hm74/NYCColumns/h9gi-nx95.VEHICLE_TYPE_CODE_4.txt.gz
