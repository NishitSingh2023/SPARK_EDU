from pyspark import SparkConf, SparkContext
import warnings
import logging
import re

warnings.filterwarnings("ignore")
logging.getLogger("py4j").setLevel(logging.ERROR)


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

sc.setLogLevel("Error")

input = sc.textFile("file:/home/nishit/Desktop/Github/SPARK_EDU/data/Book.txt")

def splitWords(Line):
    # Using regex to split words and remove punctuation
    return re.compile(r'\W+').split(Line.lower())


words = input.flatMap(splitWords)

wordCounts = words.map(lambda x: (x,1)).reduceByKey(lambda x, y: x+y)
wordCountSorted = wordCounts.map(lambda x,y: (y,x)).sortByKey()

result = wordCountSorted.collect()

for result in result:
    count = str(result[0])
    word = count.encode("ascii", "ignore").decode("utf-8")
    print(word, count)
