import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from operator import add

def normalize(v):
    max_v = v.map(lambda x: x[1]).max()
    v = v.map(lambda x: (x[0], x[1]/max_v))
    return v

def to_dict(x):
    r = dict()
    for each in x:
        r[each[0]]=each[1]
    return r

if __name__ == "__main__":
    spark = SparkSession \
            .builder \
            .appName("HITS") \
            .getOrCreate()
    sc = spark.sparkContext
    data = spark.read.text(sys.argv[1])

    lines = data.rdd.map(lambda x: x[0]).map(lambda x: x.split(' ')).map(lambda x: (x[0], x[1])).cache()
    l = lines.map(lambda x:(x[1], (x[0], 1)))
    l_t = lines.map(lambda x:(x[0], (x[1], 1)))
    node = data.rdd.map(lambda x: x[0]).map(lambda x: x.split(' ')).collect()
    n = []
    for each in node:
        n.extend(each)
    n = list(set(n))

    h0 = sc.parallelize(n).map(lambda x: (x, 1))

    iteration = sys.argv[2]
    for i in range(iteration):
        a = l_t.join(h0).map(lambda x: [x[1][0][0], x[1][0][1]*x[1][1]]).reduceByKey(add)
        a = normalize(a)
        h = l.join(a).map(lambda x: [x[1][0][0], x[1][0][1]*x[1][1]]).reduceByKey(add)
        h = normalize(h)
        h0 = h

    result_a = a.collect()
    result_h = h.collect()
    dict_a = to_dict(result_a)
    dict_h = to_dict(result_h)

    str_a = ''
    str_h = ''
    for each in sorted(n):
        if each in dict_h.keys():
            str_h += str(dict_h[each])
            str_h +=' '
        else:
            str_h += '0 '
        if each in dict_a.keys():
            str_a += str(dict_a[each])
            str_a +=' '
        else:
            str_a += '0 '

    f = open('p2.txt', 'w')
    f.write(str_h + '\n')
    f.write(str_a)
    f.close()
