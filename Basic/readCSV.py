"""
This PySpark reads data from a csv file
"""
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('Test').setMaster('local[*]')
sc = SparkContext(conf=conf)
distData = sc.textFile('data/emp/emp.csv')
distData.count()