"""
How to convert RDD to Dataframe  in pySpark programming
"""
from pyspark.sql.types import StructType,StructField,StringType
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
conf = SparkConf().setAppName('Test').setMaster('local[*]')
sc = SparkContext(conf=conf)
nums = sc.parallelize([23,12,11,552,22,66,77,88,23,555,64,6555,54,6871,6874,434,5,4,54,54,4,4,54,546,6445])
#define schema
numsSchema=StructType([StructField('num',StringType(),True)])
nums=nums.map(lambda x:(x,))
#
numsdf2=nums.toDF(schema=numsSchema)