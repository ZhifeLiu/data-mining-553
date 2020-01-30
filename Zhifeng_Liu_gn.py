import sys
from pyspark.sql import SparkSession
from pyspark import SparkContext, SparkConf
from operator import add
import networkx as nx

def groupbykey(l):
    output = dict()
    for i in l:
        if i[0] not in output:
            output[i[0]]=[i[1]]
        else:
            output[i[0]].append(i[1])
    return output
def groupby2key(l):
    output = dict()
    for i in l:
        if i[1] not in output:
            output[i[1]]=[i[0]]
        else:
            output[i[1]].append(i[0])
    return output
def get_p(child_to_parent, node, pmemo):
    if node not in child_to_parent:
        return 1
    if node in pmemo:
        return pmemo[node]
    res = 1
    for parent in child_to_parent[node]:
        res *= get_p(child_to_parent, parent, pmemo)
    pmemo[node] = res
    return res
def get_fraction(tree, child_to_parent, node, fmemo):
    if node not in tree:
        return 1
    if node in fmemo:
        return fmemo[node]
    res = 1
    for child in tree[node]:
        res += get_fraction(tree, child_to_parent, child,
                            fmemo) / len(child_to_parent[child])
    return res
def betweeness(node_tree):
    output = []
    if node_tree[0]=='E':
        node_tree[1].append(('F', 'G'))
    if node_tree[0]=='G':
        node_tree[1].append(('F', 'E'))
    node = node_tree[0]
    links = node_tree[1]
    bfs = groupbykey(links)
    rever_bfs = groupby2key(links)
    fmemo = {}
    pmemo = {}
    cur_level = [node]
    while cur_level:
        next_level = []
        for i in range(len(cur_level)):
            cur_node = cur_level[i]
            if cur_node not in bfs:
                continue
            for child in bfs[cur_node]:
                edge_fraction = get_p(rever_bfs, cur_node, pmemo) * get_fraction(bfs, rever_bfs, child, fmemo) / sum(get_p(rever_bfs, parent, pmemo) for parent in rever_bfs[child])
                output.append((tuple(sorted([cur_node, child])), edge_fraction))
                if child not in next_level:
                    next_level.append(child)
        cur_level = next_level
    return output

if __name__ == "__main__":

    spark = SparkSession \
                .builder \
                .appName("graph") \
                .getOrCreate()
    sc = spark.sparkContext
    data = spark.read.text(sys.argv[1])

    links = data.rdd.map(lambda x: x.__getitem__("value")).map(lambda x: x.split(',')).map(lambda x: tuple(x)).collect()
    G=nx.Graph()
    G.add_edges_from(links)

    results = sc.parallelize(G.nodes())\
                .map(lambda x: (x, list(nx.traversal.bfs_edges(G,x))))\
                .map(lambda x: betweeness(x))\
                .flatMap(lambda x: x)\
                .groupByKey()\
                .map(lambda x: (x[0], list(x[1])))\
                .mapValues(lambda x: sum(x)/2).sortByKey().collect()

    for each in results:
        print(str(each[0])+', '+str(each[1]))