import sys
import collections
from pyspark import SparkContext
from itertools import combinations
from pyspark.sql import SparkSession
def Pass1(local_baskets):
    candidates = []
    baskets = list(local_baskets)
    local_num = len(baskets)
    local_thre = threshold * (local_num/baskets_num)
    C1 = set()
    L1 = []
    count_table = collections.defaultdict(int)
	
    for each in baskets:
	for i in each:
	    C1.add(i)
    for item in C1:
	for basket in baskets:
	    if item in basket:
		count_table[item] += 1
	if count_table[item] >= local_thre:
            candidates.append([item])
	    L1.append([item])

    Lksub1 = L1
    k = 2

    while(True):
	Lk = []
	Ck = []
	for i in range(len(Lksub1)):
	    for j in range(i+1, len(Lksub1)):
                
	        l1 = Lksub1[i]
	        l2 = Lksub1[j]
	        l1.sort()
	        l2.sort()
                
	        if l1[0:k-2] == l2[0:k-2]:
	            Ck_item = set(Lksub1[i]) | set(Lksub1[j])

	            Ck.append(list(Ck_item))
        
        for each in Ck:
	    for basket in baskets:
		if set(each).issubset(set(basket)):
                    
		    count_table[frozenset(each)] += 1
	    if count_table[frozenset(each)] >= local_thre:
		candidates.append(each)
		Lk.append(each)
	Lksub1 = Lk
	k = k + 1
        if (len(Lksub1)==0):
            break
    return candidates
def Pass2(baskets):
    baskets = list(baskets)
    count_table = collections.defaultdict(int)
    for candidate in candidates:
        for basket in baskets:
            if(set(candidate).issubset(basket)):
                    count_table[frozenset(candidate)] += 1
    return count_table.items()
if __name__ == '__main__':
    sc = SparkContext(appName="inf553")
    file = sys.argv[1]
    threshold = sys.argv[2]
    confidence = sys.argv[3]
    header = file.first()
    baskets = file.filter(lambda x: x != header) \
        .map(lambda x: x.split(",")) \
        .map(lambda x: (x[0],int(x[1]))) \
        .groupByKey()\
        .map(lambda x: (x[0],list(x[1])))\
        .map(lambda x: x[1])\
        .collect()
    
    baskets_num = len(baskets)
    rdd = sc.parallelize(baskets)

    candidates = rdd.mapPartitions(Pass1).collect()
    results = rdd.mapPartitions(Pass2).reduceByKey(lambda x,y: x + y).filter(lambda x: x[1] >= threshold).collect()
    output = open('output.txt', 'w')

    count = {}
    for each in results:
        count[each[0]]=each[1]
    sub_list = []
    for each in results:
        if len(each[0])==1:
            sub_list.append(each[0])  
    output.write('Frequent itemset'+'\n')
    for each in candidates:
        line = ','.join(str(s) for s in each)
        output.write(line + '\n')
    output.write('Confidence'+'\n')
    for each in results:
        if len(each[0])<=1:
            continue
        for i in sub_list:
            if i.issubset(each[0]):
                conf = count[each[0]]/count[each[0]-i]
                if conf>= confidence:
                    output.write(str(tuple(each[0]-i))+ ','+ str(tuple(i))+ ' '+ str(conf)+'\n')
    
