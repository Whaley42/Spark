from pyspark.sql import SparkSession
import argparse
from itertools import combinations
import json
import time
parser = argparse.ArgumentParser(description="Apriori")
parser.add_argument("--input_file", type=str, default="./data/small2.csv",help="The input file")
parser.add_argument("--output_file", type=str, default="./data/apriori-itemsets.json",help="The output file")
parser.add_argument("--s", type=int, default=4,help="Support Threshold")
parser.add_argument("--c", type=int, default=1,help="Case number")

args = parser.parse_args()
spark = SparkSession.builder.master("local").appName("Task1").getOrCreate()
start = time.time()
start = time.time()
rdd = spark.read.csv(args.input_file).rdd.repartition(2)
print(f"Number of partitions: {rdd.getNumPartitions()}")



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


def single_candidates(transactions, local_threshold):
    candidate_count = {}
    for transaction in transactions:
        for item in transaction:
            candidate_count[item] = candidate_count.get(item, 0) + 1
    
    candidates = [tuple([item]) for item,count in candidate_count.items() if count >= local_threshold]
    return candidates

def multiple_candidates(transactions, candidates, local_threshold):
    k = 2 
    all_candidates = []
    all_candidates.append(candidates)
    while True:
       
        
        set_candidates = {num for tup in candidates for num in tup}
        distinct_candidates = list(set_candidates)
        candidates = [tuple(sorted(combo)) for combo in combinations(distinct_candidates, k)]
        candidates = set(candidates)
        candidates_count = {}
        candidates_count = {elem: 0 for elem in candidates}
        for candidate in candidates:
            for transaction in transactions:
                if set(candidate).issubset(set(transaction)):
                    candidates_count[candidate] += 1

        
        candidates = [item for item,count in candidates_count.items() if count >= local_threshold]
        all_candidates.append(candidates)
        if len(candidates) == 0:
            break
        k+=1
       
    flat_candidates = [item for sublist in all_candidates for item in sublist]
    return flat_candidates
    

def get_candidates(transactions, ps):
    transactions = [transaction for transaction in transactions]
    # print(f"Length of local transaction: {len(transactions)}")
    local_threshold = len(transactions)*ps
    # print(f"Local Threshold: {local_threshold}")
    candidates_1 = single_candidates(transactions, local_threshold)
    candidates = multiple_candidates(transactions, candidates_1, local_threshold)
    return candidates

def get_frequent_items(transactions, candidates):
    
    candidate_count = {}
    for transaction in transactions:
        for candidate in candidates:
            if set(candidate).issubset(set(transaction)):
                if candidate in candidate_count:
                    candidate_count[candidate] = candidate_count.get(candidate, 0) + 1
    
    return [(item, count) for item, count in candidate_count.items()]

def group_by_length(items):
    result={}
    for tup in items:
        size = len(tup)
        if size not in result:
            result[size] = [tup]
        else:
            result[size].append(tup)
        
    return list(result.values())
                
def split_data(transactions, start):
    s = args.s
    num_transactions = transactions.count()
    ps = float(s)/num_transactions
    
    candidates = transactions.mapPartitions(lambda x: get_candidates(x, ps))
    candidates = candidates.distinct().collect()
    
    
    frequent_itemsets = transactions.mapPartitions(lambda x: get_frequent_items(x, candidates))
    frequent_itemsets = frequent_itemsets.reduceByKey(lambda x,y: x+y)
    frequent_itemsets = frequent_itemsets.filter(lambda x: x[1] >= s)
    frequent_itemsets = frequent_itemsets.map(lambda x: x[0])
    frequent_itemsets = frequent_itemsets.collect()
   
    candidates.sort(key=lambda x: (len(x), [int(e) for e in x]))
    frequent_itemsets.sort(key=lambda x: (len(x), [int(e) for e in x]))
    candidates = group_by_length(candidates)
    frequent_itemsets = group_by_length(frequent_itemsets)
    end=time.time()
    runtime = end-start
    to_json(candidates, frequent_itemsets, runtime)
    
    

def to_json(candidates, frequent_items, runtime):
    output = {}
    output["Candidates"] = candidates
    output["Frequent Itemsets"] = frequent_items
    output["Runtime"] = runtime

    with open(args.output_file, "w") as f:
                json.dump(output, f)
   
    
def run():
    transactions = None
    if args.c == 1:
        transactions = case1(rdd)
    else:
        transactions = case2(rdd)
        
    split_data(transactions, start)

if __name__ == "__main__":
    run()
