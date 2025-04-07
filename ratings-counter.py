from pyspark import SparkConf, SparkContext
import collections
import warnings
import logging

# Suppress Python warnings
warnings.filterwarnings("ignore")

# Suppress py4j logging
logging.getLogger("py4j").setLevel(logging.ERROR)

# Spark setup
conf = SparkConf().setMaster("local").setAppName("RatingsHistogram")
sc = SparkContext(conf=conf)

# Reduce Spark log output
sc.setLogLevel("ERROR")

# Load and process the file
lines = sc.textFile("file:/home/nishit/Desktop/Github/SPARK_EDU/data/ml-100k/u.data")
ratings = lines.map(lambda x: x.split()[2])
result = ratings.countByValue()

# Sort and display results
sortedResults = collections.OrderedDict(sorted(result.items()))
for key, value in sortedResults.items():
    print("%s %i" % (key, value))
