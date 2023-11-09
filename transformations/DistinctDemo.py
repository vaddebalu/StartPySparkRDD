"""
This PySpark program to apply distinct  using RDD
"""
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('Test').setMaster('local[*]')
sc=SparkContext(conf=conf)
nums = sc.parallelize([23,12,11,11,22,23,45,223,13,13,32])
uniq=nums.distinct(2)
uniq.saveAsTextFile('uniondemo')