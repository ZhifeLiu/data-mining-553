import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
import re
import unicodedata
from operator import add


if __name__ == "__main__":
    spark = SparkSession \
            .builder \
            .appName("clustering") \
            .getOrCreate()
    sc = spark.sparkContext
    data = spark.read.text(sys.argv[1])
    fingerprint = data.rdd.map(lambda x: x.__getitem__("value"))\
        .map(lambda x: (x, 1))\
        .reduceByKey(add)\
        .map(lambda x: (x[0], x))\
        .map(lambda x: (x[0].strip(), x[1]))\
        .map(lambda x: (x[0].lower(), x[1]))\
        .map(lambda x: (re.sub(r'[^\w\s]','', x[0]), x[1]))\
        .map(lambda x: (unicodedata.normalize('NFC',x[0]), x[1]))\
        .map(lambda x: (x[0].split(' '), x[1]))\
        .map(lambda x: (list(filter(None, x[0])), x[1]))\
        .map(lambda x: (sorted(list(set(x[0]))), x[1]))\
        .map(lambda x: (' '.join(x[0]), x[1]))\
        .groupByKey()\
        .map(lambda x:(x[0], list(x[1])))\
        .map(lambda x: x[1])\
        .filter(lambda x: len(x)>=2)\
        .map(lambda x: sorted(x, key = lambda i: i[1], reverse=True))
    result = fingerprint.collect()

    f = open('p1.txt', 'w')
    for each in result:
    s = each[0] + ':'
    for i in each[1]:
        s = s + i[0] +'('+str(i[1])+')'+','
    s = s[:-1]
    print(s)
    f.write(s + '\n')
    f.close()
