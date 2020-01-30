import sys
from itertools import combinations
from pyspark.sql import SparkSession
from itertools import combinations
import operator
import csv

def min_hash(values):
    sign = []
    for i in range(1, 51):
        hash_list = []
        for v in values:
            h_value = (3*v + 11*i) % 100
            hash_list.append(h_value)
        sign.append(min(hash_list))
    return sign

def band(item):
    band = []
    for i in range(0, 10):
        band.append((i, (item[0], item[1][i])))
    return band

def candidate(input):
    result = []
    permutations = list(combinations(input, 2))
    for each in permutations:
        u1 = each[0]
        u2 = each[1]
        if operator.eq(u1[1],u2[1]):
            candi = tuple([u1[0], u2[0]])
            reverse_candi = tuple([u2[0], u1[0]])
            result.append(candi)
            result.append(reverse_candi)
    return result

def top3_user(item):
    m1 = users_dict[item[0]]
    simi_dict = {}
    top3 = []
    for each in item[1]:
        m2 = users_dict[each]
        inter = set(m1).intersection(set(m2))
        union = set(m1).union(set(m1))
        simi = float(len(inter)/len(union))
        simi_dict[simi]=each
    l = sorted(simi_dict.keys())[0:3]
    for each in l:
        top3.append(simi_dict[each])
    return (item[0], top3)

def rating_dict(values):
    d = {}
    for each in values:
        d[each[0]]=each[1]
    return d
def predict_movies(item):
    u1 = ratings_dict[item[0]].keys()
    pred_movies = []
    for each in item[1]:
        u2 = ratings_dict[each].keys()
        diff = set(u2).difference(set(u1))
        pred_movies.extend(list(diff))
    pred_movies = list(set(pred_movies))
    pred_ratings = {}
    for i in pred_movies:
        pred_ratings[i] = []
        for each in item[1]:
            if i in ratings_dict[each].keys():
                pred_ratings[i].append(ratings_dict[each][i])
            else:
                continue
    return (int(item[0]), pred_ratings)
def average_rating(values):
    for each in values:
        values[each] = float(sum(values[each])/len(values[each]))
    values = sorted(values.items(),key=lambda item:item[0])
    return values
if __name__ == "__main__":
	
    spark = SparkSession \
            .builder \
            .appName("LSH") \
            .getOrCreate()
    sc = spark.sparkContext

    lines = sc.textFile(sys.argv[1])
    header = lines.first()
    data = lines.filter(lambda x: x != header).map(lambda x: x.split(',')).collect()
    rdd = sc.parallelize(data)
    baskets = rdd.filter(lambda x: x != header) \
            .map(lambda x: (x[0],int(x[1]))) \
            .groupByKey()\
            .map(lambda x: (x[0],list(x[1]))).collect()
    signiture = sc.parallelize(baskets).mapValues(lambda values: min_hash(values))
    signiture = signiture.mapValues(lambda values: [values[i:i+5] for i in range(0, 50, 5)]).collect()

    bands = sc.parallelize(signiture).flatMap(lambda x: band(x)).groupByKey().map(lambda x:(x[0], list(x[1]))).collect()

    users_dict = {}
    for each in baskets:
        users_dict[each[0]]=each[1]


    candidates = sc.parallelize(bands).mapValues(lambda values: candidate(values)).flatMap(lambda x: x[1]).distinct().groupByKey().map(lambda x: (x[0], list(x[1]))).collect()
    top3_simi = sc.parallelize(candidates).map(lambda x: top3_user(x)).collect()
    ratings = rdd.map(lambda x: (x[0],(int(x[1]), float(x[2])))) \
            .groupByKey()\
            .map(lambda x: (x[0],list(x[1]))).mapValues(lambda v: rating_dict(v)).collect()

    ratings_dict = {}
    for each in ratings:
        ratings_dict[each[0]]=each[1]

    predicting = sc.parallelize(top3_simi).map(lambda x: predict_movies(x)).mapValues(lambda x : average_rating(x)).sortByKey().collect()


    f = open(sys.argv[2], 'w')
    csv_writer = csv.writer(f)
    csv_writer.writerow(['user', 'movie', 'rating'])
    for i in range(len(predicting)):
        for each in predicting[i][1]:
            csv_writer.writerow([predicting[i][0], each[0], each[1]])
    f.close()
