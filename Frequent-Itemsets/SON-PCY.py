from itertools import combinations
import json
from pyspark.sql import SparkSession
import argparse
import time

parser = argparse.ArgumentParser(description="SON Algorithm Using PCY")
parser.add_argument("--input_file", type=str, default="./data/nevada.csv",help="The input file")
parser.add_argument("--output_file", type=str, default="./data/itemsets.json",help="The output file")
parser.add_argument("--s", type=int, default=10,help="Support Threshold")
parser.add_argument("--k", type=int, default=10,help="k reviews")
args = parser.parse_args()
num_buckets = 100000


def hash_combo(combo):
    key = ""
    if combo[0] < combo[1]:
        key = combo[0] + combo[1]
    else:
        key = combo[1] + combo[0]
    
    return hash(key) % num_buckets


def prune_candidates(candidates, k):
    pruned_candidates = []
    for i,candidate1 in enumerate(candidates[:len(candidates)-1]):  #  Outer Loop that goes through all the candidates as a set
        for candidate2 in candidates[i + 1:]:  #  Inner loop that starts at the next element and goes to end of list. 
            if candidate1[:k-1] != candidate2[:k-1]:  #  If The first k-1 aren't the same, break, we only want matching
                break
            new_combo = set(candidate1).union(set(candidate2))
            if len(new_combo) != k+1:
                continue       
            new_combo_sorted = tuple(sorted(list(new_combo)))
            new_combo_combos = list(combinations(new_combo_sorted,k)) 
            if set(new_combo_combos).issubset(set(candidates)):
                pruned_candidates.append(new_combo_sorted)    
    
    return pruned_candidates


def reduce_transaction(transaction, single_candidates):
    intersect = set(transaction).intersection(set(single_candidates))
    return sorted(list(intersect))


def get_candidates(transactions, s, total_transactions):

    ps = (float(s)/total_transactions)
    transactions = [transaction for transaction in transactions]
    local_threshold = ps * len(transactions)
    
    candidates_1, bitmap = single_candidates(transactions, local_threshold)
    all_candidates = multiple_candidates(transactions, candidates_1, bitmap, local_threshold)
    return all_candidates
    


def multiple_candidates(transactions, candidates_1, bitmap, local_threshold):
    
    all_candidates = []
    candidates_1_formatted = [tuple(item.split(",")) for item in candidates_1]
    all_candidates.append(candidates_1_formatted)

    k = 2
    while True:
        print(f"K: {k}")
        
        candidates_count = {}
        for transaction in transactions:
            
            transaction = reduce_transaction(transaction, candidates_1)
            if k == 2:
                for combo in combinations(transaction, k):
                    if bitmap[hash_combo(combo)]:
                        candidates_count[candidate] = candidates_count.get(candidate, 0) + 1
            else:
                for candidate in new_candidates:
                    if set(candidate).issubset(set(transaction)):
                        if candidate in candidates_count:
                            candidates_count[candidate] = candidates_count.get(candidate, 0) + 1

        candidates = [item for item,count in candidates_count.items() if count >= local_threshold]
        candidates = list(set(candidates))
        if len(candidates) == 0:
            break
        new_candidates = prune_candidates(sorted(candidates),k)
        all_candidates.append(candidates)
        k += 1
  
    
    return [item for sublist in all_candidates for item in sublist]
    


def single_candidates(transactions, support):

    hashmap = [0 for _ in range(num_buckets)]
    candidates_count = {}
    for transaction in transactions:
        for item in transaction:
            candidates_count[item] = candidates_count.get(item, 0) + 1
                
        for combo in combinations(transaction, 2):
            index = hash_combo(combo)
            hashmap[index] += 1

    candidates = [item for item,count in candidates_count.items() if count >= support]
    candidates = sorted(candidates)
    bitmap = [1 if count >= support else 0 for count in hashmap]
    return candidates, bitmap

def get_frequent_itemsets(transactions, candidates):
    transactions = [transaction for transaction in transactions]
    candidates_count = {}
    for transaction in transactions:
        for candidate in candidates:
            if set(candidate).issubset(set(transaction)):
                candidates_count[candidate] = candidates_count.get(candidate, 0) + 1
    return [(item,count) for item,count in candidates_count.items()]
    


def group_by_length(items):
    result={}
    for tup in items:
        size = len(tup)
        if size not in result:
            result[size] = [tup]
        else:
            result[size].append(tup)
        
    return list(result.values())


def to_json(candidates_output, frequent_items, runtime):
    output = {}
    output["Candidates"] = candidates_output
    output["Frequent Itemsets"] = frequent_items
    output["Runtime"] = runtime

    with open(args.output_file, "w") as f:
                json.dump(output, f)
                
def create_rdd():
    spark = SparkSession.builder.master("local").appName("Son").getOrCreate()
    rdd = spark.read.csv(args.input_file).rdd.repartition(2)
    rdd = rdd.map(lambda x: (x[0],x[1]))
    rdd = rdd.filter(lambda x: x[0] != "user_id")
    rdd = rdd.groupByKey().mapValues(set)
    transactions = rdd.map(lambda x: list(x[1]))
    transactions = transactions.filter(lambda x: len(x) > args.k)

    return transactions


if __name__ == '__main__':
    start = time.time()
    transactions = create_rdd()
    total_transactions = transactions.count()

    candidates = transactions.mapPartitions(lambda x: get_candidates(x,args.s,total_transactions))
    candidates_collected = candidates.sortBy(lambda x: (len(x), x)).distinct().collect()
    candidates_output = group_by_length(candidates_collected)
    
    frequent_itemsets = transactions.mapPartitions(lambda x: get_frequent_itemsets(x, candidates_collected))
    frequent_itemsets = frequent_itemsets.reduceByKey(lambda x,y: x+y)
    frequent_itemsets = frequent_itemsets.filter(lambda x: x[1] >= args.s)
    frequent_itemsets = frequent_itemsets.map(lambda x: x[0])
    frequent_itemsets = frequent_itemsets.sortBy(lambda x: (len(x),x)).collect()
    frequent_itemsets = group_by_length(frequent_itemsets)
    end = time.time()
    runtime = round(end - start,2)
    to_json(candidates_output, frequent_itemsets, runtime)

