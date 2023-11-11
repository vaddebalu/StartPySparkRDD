"""
convert RDD into dataframe using toDF() function in PySpark
"""
from pyspark.sql.types import StructType,StructField,StringType
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
#define schema
numsSchema=StructType([StructField('num',StringType(),True)])

#

spark=SparkSession.builder.getOrCreate()
nums = spark.sparkContext.parallelize([23,12,11,552,22,66,77,88,23,555,64,6555,54,6871,6874,434,5,4,54,54,4,4,54,546,6445])
numsdf2=nums.toDF(schema=numsSchema)
numsdf2.printSchema()