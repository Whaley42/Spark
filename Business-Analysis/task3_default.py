from pyspark.sql import SparkSession
import argparse
import json

parser = argparse.ArgumentParser(description="T3D")
parser.add_argument("--input_file", type=str, default="./data/review.json",help="Input file")
parser.add_argument("--output_file", type=str, default="./data/t3_default.json",help="The output file")
parser.add_argument("--n", type=int, default=10,help="n reviews")
args = parser.parse_args()
spark = SparkSession.builder.master("local").appName("Task3D").getOrCreate()
reviews = spark.read.json(args.input_file).rdd

class t3_default:
    
    def more_than_n_reviews(self):
    
        reviews_mapped = reviews.map(lambda x: (x["business_id"], 1))
        reviews_reduce = reviews_mapped.reduceByKey(lambda x,y: x+y)
        reviews_filtered = reviews_reduce.filter(lambda x: x[1] > args.n)
        greater_n_reviews = reviews_filtered.collect()

        partition_count = reviews.getNumPartitions()
        n_items = reviews.glom().map(len).collect()
        
        return partition_count, n_items, greater_n_reviews, 

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
    t = t3_default()
    t.run()