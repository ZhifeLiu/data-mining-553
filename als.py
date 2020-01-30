#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
This is an example implementation of ALS for learning how to use Spark. Please refer to
pyspark.ml.recommendation.ALS for more conventional use.

This example requires numpy (http://www.numpy.org/)
"""
import csv
import numpy as np
import sys
from numpy.random import rand
from numpy import matrix
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf

LAMBDA = 0.01   # regularization
np.random.seed(42)


def rmse(R, ms, us):
    diff = R - ms * us.T
    return np.sqrt(np.sum(np.power(diff, 2)) / (M * U))


def update(i, mat, ratings):
    uu = mat.shape[0]
    ff = mat.shape[1]
#
    XtX = mat.T * mat
    Xty = mat.T * ratings[i, :].T
#
    for j in range(ff):
        XtX[j, j] += LAMBDA * uu
#
    return np.linalg.solve(XtX, Xty)


if __name__ == '__main__':
    conf = SparkConf().setAppName("ALS").setMaster("local[4]")
    sc = SparkContext.getOrCreate(conf)
    spark = SparkSession\
        .builder\
        .appName("ALS")\
        .getOrCreate()
    data = spark.read.csv(sys.argv[1], header = True)
    baskets = data.rdd.map(lambda row: (int(row['userId']),int(row['movieId']),float(row['rating']))).collect()
    rating_dict={}
    for each in baskets:
        rating_dict[(each[0],each[1])] = each[2]
    
    users_id = data.rdd.map(lambda row: int(row['userId'])).distinct()
    movies_id = data.rdd.map(lambda row: int(row['movieId'])).distinct()
    sort_user_id = np.sort(users_id.collect())
    sort_movie_id = np.sort(movies_id.collect())
    user_num = users_id.count()
    movie_num = movies_id.count()
    
    M = user_num
    U = movie_num
    F = 5
    ITERATIONS = 20
    partitions = 5
    
    R = matrix(rand(M, F)) * matrix(rand(U, F).T)
    ms = matrix(rand(M, F))
    us = matrix(rand(U, F))
    
    for i in range(M):
        row = []
        for j in range(U):
            user_index = sort_user_id[i]
            movie_index = sort_movie_id[j]
            if (user_index,movie_index) in rating_dict.keys():
                row.append(rating_dict[(user_index,movie_index)])
            else:
                row.append(0)
        R[i] = row
            
    Rb = sc.broadcast(R)
    msb = sc.broadcast(ms)
    usb = sc.broadcast(us)
    
    for i in range(ITERATIONS):
        ms = sc.parallelize(range(M), partitions) \
               .map(lambda x: update(x, usb.value, Rb.value)) \
               .collect()
        # collect() returns a list, so array ends up being
        # a 3-d array, we take the first 2 dims for the matrix
        ms = matrix(np.array(ms)[:, :, 0])
        msb = sc.broadcast(ms)

        us = sc.parallelize(range(U), partitions) \
               .map(lambda x: update(x, msb.value, Rb.value.T)) \
               .collect()
        us = matrix(np.array(us)[:, :, 0])
        usb = sc.broadcast(us)

        error = rmse(R, ms, us)
    S = ms * us.T

    f = open(sys.argv[2], 'w')
    csv_writer = csv.writer(f)
    csv_writer.writerow(['user', 'movie', 'rating'])
    for i in range(M):
        for j in range(U):
            if R[i].item(j) != 0:
                    continue
            else:
                csv_writer.writerow([sort_user_id[i], sort_movie_id[j], S[i].item(j)])
    spark.stop()
