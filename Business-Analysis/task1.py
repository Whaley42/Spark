from pyspark.sql import SparkSession
import argparse
import json
import re
parser = argparse.ArgumentParser(description="T1")
parser.add_argument("--input_file", type=str, default="./data/review.json",help="The input file")
parser.add_argument("--output_file", type=str, default="./data/t1.json",help="The output file")
parser.add_argument("--stopwords", type=str, default="./data/stopwords",help="The stopwords file")
parser.add_argument("--y", type=int, default=2018,help="Year")
parser.add_argument("--m", type=int, default=10,help="m most users")
parser.add_argument("--n", type=int, default=10,help="n most frequent words")
args = parser.parse_args()
spark = SparkSession.builder.master("local").appName("Task1").getOrCreate()
spark_reviews = spark.read.json(args.input_file).rdd

class task1:
    
    def get_total_reviews(self):
        total_reviews = spark_reviews.count()
        return total_reviews

    def review_by_year(self):
        reviews_rdd = spark_reviews.filter(lambda x: x["date"][:4] == str(args.y))
        reviews_count = reviews_rdd.count()
        return reviews_count

    def unique_users(self):
        unique_users_rdd = spark_reviews.map(lambda x: x["user_id"]).distinct()
        unique_count = unique_users_rdd.count()
        return unique_count

    def most_user_reviews(self):
        user_reviews_rdd = spark_reviews.map(lambda x: (x["user_id"], 1))

        reduced_users = user_reviews_rdd.reduceByKey(lambda x, y: x + y)
        users_sorted = reduced_users.sortBy(lambda x: (-x[1],x[0]))
        most_user_reviews = users_sorted.take(args.m)
        return most_user_reviews

    def frequent_words(self):

        punctuation = ["(", "[", ",", ".", "!", "?", ":", ";", "]", ")", " "] 
        pattern = "|".join(re.escape(p) for p in punctuation)

        rdd_words = spark_reviews.flatMap(lambda x: re.split(pattern, x["text"].lower()))

        with open(args.stopwords) as f:
            stop_words=[word for line in f for word in line.split()]
        
        reviews_filtered = rdd_words.filter(lambda x: x not in [""] and x not in stop_words)
        rdd_words_mapped = reviews_filtered.map(lambda x: (x, 1))
        rdd_reduced_words = rdd_words_mapped.reduceByKey(lambda x, y: x + y)
        rdd_words_sorted = rdd_reduced_words.sortBy(lambda x: x[1], ascending=False)
        frequent_words = rdd_words_sorted.map(lambda x: x[0])
        frequent_words = frequent_words.take(args.n)
        return frequent_words
    
    def to_json(self, total_reviews, reviews_by_year, unique_users, most_user_reviews, frequent_words):
        output = {}
        output["A"] = total_reviews
        output["B"] = reviews_by_year
        output["C"] = unique_users
        output["D"] = most_user_reviews
        output["E"] = frequent_words

        with open(args.output_file, "w") as f:
            f.write(json.dumps(output))
    
    def run(self):
        total_reviews = self.get_total_reviews()
        reviews_by_year = self.review_by_year()
        unique_count = self.unique_users()
        most_reviews = self.most_user_reviews()
        top_words = self.frequent_words()
        self.to_json(total_reviews, reviews_by_year, unique_count, most_reviews, top_words)


if __name__ == "__main__":
    t = task1()
    t.run()
    