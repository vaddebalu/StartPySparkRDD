"""
This PySpark program to apply pipe using RDD
"""
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('Test').setMaster('local[*]')
sc=SparkContext(conf=conf)
nums = sc.parallelize([23,12,11,552,22,66,77,88,23,555,64,6555,54,6871,6874,434,5,4,54,54,4,4,54,546,6445])
#apply filter using pipe function to use shell command
num4=nums.pipe('grep 4')
num4.saveAsTextFile('pipe')