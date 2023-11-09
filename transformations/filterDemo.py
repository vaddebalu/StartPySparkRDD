"""
This PySpark program to apply filter using RDD
"""
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('Test').setMaster('local[*]')
sc=SparkContext(conf=conf)
distData = sc.parallelize([23,12,11,552,22,66,77,88,23,555,64,6555,54,6871,6874,434,5,4,54,54,4,4,54,546,6445])
#apply filter using lambda function
num50=distData.filter(lambda x:x>=50)
num50.saveAsTextFile('num50')