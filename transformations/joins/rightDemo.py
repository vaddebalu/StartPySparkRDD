"""
This PySpark program shows how to apply right outer Join using RDD
"""
from operator import add
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('Test').setMaster('local[*]')
sc=SparkContext(conf=conf)
emp = sc.parallelize([('Balu',10),('Bala',11),('Sai',12),('Swamy',13),('Nitya',200000)])
empsalary=sc.parallelize([('Balu',100000),('Bala',110000),('Sai',12500),('Swamy',100000),('Nirupam',120000)])
#apply right outer join
empJoin=emp.rightOuterJoin(empsalary)
empJoin.saveAsTextFile('empJoin')