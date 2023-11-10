"""
This PySpark program shows how to apply sortByKey using RDD
"""
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('Test').setMaster('local[*]')
sc=SparkContext(conf=conf)
distData = sc.parallelize([23,12,11,552,22,66,77,88,23,555,64,6555,54,6871,6874,434,5,4,54,54,4,4,54,546,6445])
mapped=distData.map(lambda x:(x,1))
#apply reduceByKey using add operator
sorted=mapped.sortByKey()
sorted.saveAsTextFile('num50')