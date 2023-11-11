"""
This PySpark program to apply repartition using RDD
"""
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('Test').setMaster('local[*]')
sc=SparkContext(conf=conf)
nums = sc.parallelize([23,12,11,552,22,66,77,88,23,555,64,6555,54,6871,6874,434,5,4,54,54,4,4,54,546,6445])
#apply filter using repartition function to reduce partitions
nums=nums.repartition(5)
mapped=nums.map(lambda x:(x,1))
mapped.saveAsTextFile('repartition')