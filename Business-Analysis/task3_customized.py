from pyspark.sql import SparkSession
import argparse
import json
import time

parser = argparse.ArgumentParser(description="T3C")
parser.add_argument("--input_file", type=str, default="./data/review.json",help="The review file")
parser.add_argument("--output_file", type=str, default="./data/t3_custom.json",help="The output file")
parser.add_argument("--n", type=int, default=10,help="The input file")
parser.add_argument("--n_partitions", type=int, default=11,help="The input file")
args = parser.parse_args()
spark = SparkSession.builder.master("local").appName("Task3C").getOrCreate()

class t3_custom:
    
    def custom_partition(self, key):
        return hash(key) % args.n_partitions

    def more_than_n_reviews(self):
    
        rdd = spark.read.json(args.input_file).rdd

        reviews = rdd.map(lambda x: (x["business_id"], 1)).partitionBy(args.n_partitions, self.custom_partition)
        reviews_reduced = reviews.reduceByKey(lambda x,y: x+y)     
        reviews_filtered = reviews_reduced.filter(lambda x: x[1] > args.n)
        
        greater_n_reviews = reviews_filtered.collect()
        partition_count = reviews_filtered.getNumPartitions()    
        n_items = reviews_filtered.glom().map(len).collect()
        
        return partition_count, n_items, greater_n_reviews

    def to_json(self, partition_count, n_items, greater_n_reviews):
        output = {}
        output["n_partitions"] = partition_count 
        output["n_items"] = n_items
        output["result"] = greater_n_reviews

        with open(args.output_file, "w") as f:
            f.write(json.dumps(output))
        

    def run(self):
        partition_count, n_items, greater_n_reviews = self.more_than_n_reviews()
        self.to_json(partition_count, n_items, greater_n_reviews)

        
        
if __name__ == "__main__":
    t = t3_custom()
    t.run()



