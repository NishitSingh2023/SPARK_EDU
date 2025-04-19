from pyspark import SparkConf, SparkContext
import warnings
import logging 

warnings.filterwarnings("ignore")
logging.getLogger("py4j").setLevel(logging.ERROR)


conf = SparkConf().setMaster("local").setAppName("WordCount")
sc = SparkContext(conf=conf)

sc.setLogLevel("Error")

input = sc.textFile("file:/home/nishit/Desktop/Github/SPARK_EDU/data/Book.txt")

words = input.flatMap(lambda x: x.split())
wordCounts = words.countByValue()

for words, count in wordCounts.items():
    cleanWord = words.encode("ascii", "ignore").decode("utf-8")
    print(cleanWord, count)