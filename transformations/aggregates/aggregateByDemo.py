"""
This PySpark program shows how to apply groupbykey using RDD
"""
from operator import add
from pyspark import SparkContext, SparkConf
conf = SparkConf().setAppName('Test').setMaster('local[*]')
sc=SparkContext(conf=conf)
# Sample data
data = [("apple", 3), ("orange", 5), ("apple", 7), ("banana", 2), ("orange", 8)]

# Creating a Pair RDD
rdd = sc.parallelize(data)


# Define zeroValue, seqOp, and combOp functions
zero_value = (0, 0)  # (sum, count)
def seqop(acc,value):
    print('before seq function')
    print(acc,value)
    return acc+value

def combop(acc1,acc2):
    print('before comb function ...')
    print(acc1,acc2)
    return acc1+acc2

# Using aggregateByKey to find the total count for each word
result = rdd.aggregateByKey(zero_value, seqop, combop)

# Save the result
result.saveAsTextFile('num50')