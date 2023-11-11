"""
Use RDD reduce()  action in pySpark programming
"""

from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('Test').setMaster('local[*]')
sc = SparkContext(conf=conf)
nums = sc.parallelize([23,12,11,552,22,66,77,88,23,555,64,6555,54,6871,6874,434,5,4,54,54,4,4,54,546,6445])
#use reduce() function to get sum of all nums
nums.reduce(lambda x,y:x+y)