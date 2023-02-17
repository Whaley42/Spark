from pyspark.sql import SparkSession
from pyspark import SparkContext
import argparse
from itertools import combinations
import json
import time
parser = argparse.ArgumentParser(description="T2")
parser.add_argument("--input_file", type=str, default="./data/small1.csv",help="The input file")
parser.add_argument("--output_file", type=str, default="./data/a2t1.json",help="The output file")
parser.add_argument("--s", type=int, default=3,help="Support Threshold")
parser.add_argument("--c", type=int, default=1,help="Case number")

args = parser.parse_args()
spark = SparkSession.builder.master("local").appName("Task1").getOrCreate()
start = time.time()
rdd = spark.read.csv(args.input_file).rdd

def contains_combo(transaction, items, k):
    valid_combos = []
    # print(items)
    for combo in get_combinations(items,k):
        is_valid = True
        for i in range(0, len(combo)):
            if combo[i] not in transaction:
                is_valid = False
                break
        if is_valid:
            valid_combos.append((combo,1))
    
    return valid_combos    

def find_combo(transaction, candidates):
    
    valid_candidates = []
    for candidate in candidates:
        is_valid = True
        for i in range(0, len(candidate)):
            if candidate[i] not in transaction:
                is_valid = False
                break
        if is_valid:
            valid_candidates.append((candidate, 1))
    return valid_candidates


def get_combinations(items, size):
    # Define a generator to yield each combination
    for combination in combinations(items, size):
        yield combination

def make_combinations(items, size):
    combination = combinations(items, size)
    return list(combination)  

def case1(rdd):
    rdd = rdd.map(lambda x: (x[0],x[1]))
    rdd = rdd.filter(lambda x: x[0] != "user_id")
    rdd = rdd.groupByKey().mapValues(set)
    transactions = rdd.map(lambda x: list(x[1]))
    return transactions

def case2(rdd):
    rdd = rdd.map(lambda x: (x[1],x[0]))
    rdd = rdd.filter(lambda x: x[0] != "business_id")
    rdd = rdd.groupByKey().mapValues(set)
    transactions = rdd.map(lambda x: list(x[1]))
    return transactions


def frequent_items(transactions, local_threshold):
    
    frequent_itemsets = []
    candidates = transactions.flatMap(lambda x: make_combinations(x, 1))
    candidates = candidates.map(lambda x: (x,1))
    candidates = candidates.reduceByKey(lambda x, y: x+y)
    candidates_filtered = candidates.filter(lambda x: x[1] >= local_threshold)

    candidates = candidates_filtered.map(lambda x: x[0])
    frequent_itemsets.extend(candidates.collect())
    candidates = candidates.flatMap(lambda x: list(x))
    

    k=2
    while True:
        combos = candidates.collect()
        # combos = get_combinations(combos, k)

        candidates = transactions.flatMap(lambda x: [(contains_combo(x, combos, k))])
        candidates = candidates.flatMap(lambda x: x)

        candidates = candidates.reduceByKey(lambda x,y: x +y)
        
        
        candidates = candidates.filter(lambda x: x[1] >= local_threshold)
        items = candidates.map(lambda x: x[0])
        if items.count() == 0:
            break
        frequent_itemsets.extend(items.collect())
        candidates = candidates.flatMap(lambda x: x[0]).distinct()

        k+=1
    
    return spark.sparkContext.parallelize(frequent_itemsets)
    
def to_json(candidates, frequent_items, runtime):
    output = {}
    output["Candidates"] = candidates
    output["Frequent Itemsets"] = frequent_items
    output["Runtime"] = runtime
    with open(args.output_file, "w") as f:
                json.dump(output, f)
                
def split_data(transactions, start):
    s = args.s
    local_threshold = s * 0.5
    partitions = transactions.randomSplit([0.5,0.5])
    candidates = [frequent_items(partition, local_threshold) for partition in partitions]
    grouped_candidates = spark.sparkContext.union(candidates).distinct()
    filter_data(grouped_candidates, transactions, start)
   
def filter_data(candidates, transactions, start):
    print(candidates.collect())
    frequent_items = []
    #Start at k = 1, check if candidates with size 1 are in each basket
    k = 1
    k_candidates = candidates.filter(lambda x: len(x) == k).flatMap(lambda x: x)
    curr_candidates = k_candidates.collect()
    grouped_candidates = transactions.flatMap(lambda x: [(contains_combo(x, curr_candidates, k))]).flatMap(lambda x: x)
    reduced = grouped_candidates.reduceByKey(lambda x,y: x+y)
    filtered = reduced.filter(lambda x: x[1] >= args.s)
    itemsets = filtered.map(lambda x: x[0])
    frequent_items.append(itemsets)
    
    k += 1
    while True:
        k_candidates = candidates.filter(lambda x: len(x) == k)
        if k_candidates.count() == 0:
            break
        curr_candidates = k_candidates.collect()
        # print(curr_candidates)
        grouped_candidates = transactions.flatMap(lambda x: [(find_combo(x, curr_candidates))]).flatMap(lambda x: x)
        # print(grouped_candidates.collect())
        reduced = grouped_candidates.reduceByKey(lambda x,y: x+y)
        
        filtered = reduced.filter(lambda x: x[1]>=args.s)
        itemsets = filtered.map(lambda x: x[0])
        frequent_items.append(itemsets)
        
        k+=1
    end = time.time()
    runtime = end - start
    print(runtime)
    candidates = candidates.sortBy(lambda x: (len(x),[int(num) for num in x]))
  
    frequent_items_rdd = spark.sparkContext.union(frequent_items)
    
    frequent_items_rdd = frequent_items_rdd.sortBy(lambda x: (len(x), [int(num) for num in x]))
    
    to_json(candidates.collect(), frequent_items_rdd.collect(), runtime)
    
    
    
def run():
    start = time.time()
    transactions = None
    if args.c == 1:
        transactions = case1(rdd)
    else:
        transactions = case2(rdd)
    transactions = spark.sparkContext.parallelize([["1","2","3"], ["1","2"], ["1","4","5"], ["2","3"], ["1","3","5"], 
                                                   ["2","5"], ["3","5"], ["1","4","5"], ["1","2","3"],
                                                   ["4","5","6"], ["3","5","7"], ["1","3","6"], ["1","5"], ["2","7"], 
                                                   ["2","3","4"], ["1","4","6"], ["3", "1"], ["3", "1"]])
    split_data(transactions, start)
    # first_pass_candidates, frequent_itemsets, runtime = frequent_items(transactions)
    # to_json(first_pass_candidates, frequent_itemsets, runtime)
    
if __name__ == "__main__":
    run()
