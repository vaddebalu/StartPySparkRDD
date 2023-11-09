"""
This PySpark program to apply union  using RDD
"""
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('Test').setMaster('local[*]')
sc=SparkContext(conf=conf)
num1 = sc.parallelize([23,12,11])
num2=sc.parallelize([64,6555,54])
#apply filter using lambda function
bothnums=num1.union(num2)
#out is like [(20)
#               ,(1)]
bothnums.saveAsTextFile('uniondemo')