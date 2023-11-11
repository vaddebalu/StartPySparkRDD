"""
How to convert RDD to Dataframe  in pySpark programming
"""
from pyspark.sql.types import StructType,StructField,IntegerType
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
conf = SparkConf().setAppName('Test').setMaster('local[*]')
sc = SparkContext(conf=conf)
nums = sc.parallelize([23,12,11,552,22,66,77,88,23,555,64,6555,54,6871,6874,434,5,4,54,54,4,4,54,546,6445])
numsSchema=StructType([StructField('num',IntegerType(),True)])
nums=nums.map(lambda x:(x,))
#convert nums rdd to dataframe
spark=SparkSession.builder.getOrCreate()
numsDF=spark.createDataFrame(nums,schema=numsSchema)
numsDF.show()