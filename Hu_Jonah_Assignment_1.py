from __future__ import print_function

import os
import sys
import requests
from operator import add

from pyspark import SparkConf,SparkContext
from pyspark.streaming import StreamingContext

from pyspark.sql import SparkSession
from pyspark.sql import SQLContext

from pyspark.sql.types import *
from pyspark.sql import functions as func
from pyspark.sql.functions import *


#Exception Handling and removing wrong datalines
def isfloat(value):
    try:
        float(value)
        return True
 
    except:
         return False

#Function - Cleaning
#For example, remove lines if they don’t have 16 values and 
# checking if the trip distance and fare amount is a float number
# checking if the trip duration is more than a minute, trip distance is more than 0 miles, 
# fare amount and total amount are more than 0 dollars
def correctRows(p):
    if(len(p)==17):
        if(isfloat(p[5]) and isfloat(p[11])):
            if(float(p[4])> 60 and float(p[5])>0 and float(p[11])> 0 and float(p[16])> 0):
                return p

#Main
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print("Usage: main_task1 <file> <output> ", file=sys.stderr)
        exit(-1)
    
    sc = SparkContext(appName="Assignment-1")
    
    raw_rdd = sc.textFile(sys.argv[1])

    ### Task 1 ###

    # Clean data
    # q1,q2,q3,q4 -> (q1, q2, q3, q4)
    clean_rdd = raw_rdd.map(lambda x: x.split(',')).filter(correctRows)

    # Get distinct id pairs
    # (q1, q2, q3, q4) -> (q1, q2)
    medallion_license_rdd = clean_rdd.map(lambda x: (x[0], x[1])).distinct()

    # MapReduce by Key
    # (q1, q2) -> (q1, 1), (q2, 1) -> (q1, sum), (q2, sum)
    result = medallion_license_rdd.map(lambda x: (x[0], 1)).reduceByKey(add)

    # Get top 10 overalls counts
    results_1 = result.takeOrdered(10, key=lambda x: -x[1])

    #convert to rdd and savings output to argument
    sc.parallelize(results_1).coalesce(1).saveAsTextFile(sys.argv[2])

    ### Task 2 ###
    
    # money/min = total_amount / (trip_secs / 60)
    driver_rdd = clean_rdd.map(lambda x: (x[1], float(x[16]) / (float(x[4]) / 60.0)))

    # MapReduce
    # (id, (money/min, 1)) -> (id, (sum(money/min), sum(total count))) -> (id, sum(money/min) / sum(total count))
    result = driver_rdd.map(lambda x: (x[0], (x[1], 1)))\
                    .reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))\
                    .mapValues(lambda x: x[0] / x[1])

    # Get top 10 overalls counts
    results_2 = result.takeOrdered(10, key=lambda x: -x[1])

    #revert to rdd savings output to argument
    sc.parallelize(results_2).coalesce(1).saveAsTextFile(sys.argv[3])

    #Task 3 - Optional 
    #Your code goes here

    #Task 4 - Optional 
    #Your code goes here

    sc.stop()