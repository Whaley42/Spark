from pyspark.sql import SparkSession
import argparse
import json
parser = argparse.ArgumentParser(description="T2")
parser.add_argument("--review_file", type=str, default="./data/review.json",help="The review file")
parser.add_argument("--business_file", type=str, default="./data/business.json",help="The business file")
parser.add_argument("--output_file", type=str, default="./data/t2.json",help="The output file")
parser.add_argument("--n", type=int, default=50,help="n ratings")

args = parser.parse_args()
spark = SparkSession.builder.master("local").appName("Task2").getOrCreate()
spark_reviews = spark.read.json(args.review_file).rdd
spark_business = spark.read.json(args.business_file).rdd

class task2:

    def top_rated_categories(self):
        review_pairs = spark_reviews.map(lambda x: (x["business_id"], x["stars"]))
        business_pairs = spark_business.map(lambda x: (x["business_id"], x["categories"]))

        #Filter our the Null values for businesses that have Null categories
        business_filtered = business_pairs.filter(lambda x: x[1] is not None)

        #Split the categories by , and strip any extra space.
        business_filtered = business_filtered.map(lambda x: (x[0], [category.strip() for category in x[1].split(", ")]))

        #Group by business_id for the key, and each category associated with that business 
        business_categories = business_filtered.flatMap(lambda x: [(x[0], category) for category in x[1]])

        #Join the two RDDs together
        rdd_join = review_pairs.join(business_categories)

        #Group by (category, (stars, 1))
        rdd_categories = rdd_join.map(lambda x: (x[1][1], (x[1][0], 1)))

        #Reduce by key (category) and have the values as (sum(stars), sum(count))
        rdd_reduced = rdd_categories.reduceByKey(lambda x, y: (x[0] + y[0], x[1] + y[1]))

        #Transform the rdd so the key: category and values: sum(stars)/sum(count)
        rdd_average = rdd_reduced.map(lambda x: (x[0], x[1][0]/x[1][1]))

        #Sort the stars first, by descending order, then if they are equal, sort by category, ascending. 
        rdd_sorted = rdd_average.sortBy(lambda x: (-x[1], x[0]))

        n_ratings = rdd_sorted.take(args.n)
        return n_ratings

    
    def to_json(self, n_ratings):
        output2 = {}
        output2["result"] = n_ratings
        
        with open(args.output_file, "w") as f:
            f.write(json.dumps(output2))
    
    def run(self):
        n_ratings = self.top_rated_categories()
        self.to_json(n_ratings)

if __name__ == "__main__":
    t = task2()
    t.run()


