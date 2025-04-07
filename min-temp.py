from pyspark import SparkConf, SparkContext
import warnings
import collections
import logging 

warnings.filterwarnings("ignore")
logging.getLogger("py4j").setLevel(logging.ERROR)

conf = SparkConf().setMaster("local").setAppName("MinTemps")
sc = SparkContext(conf=conf)

sc.setLogLevel("Error")

def parseLine(line):
    fields = line.split(',')
    station_id = fields[0]
    entry_type = fields[2]
    temp = float(fields[3])
    return station_id,entry_type,temp

lines = sc.textFile("file:/home/nishit/Desktop/Github/SPARK_EDU/data/1800.csv")
parsedLines = lines.map(parseLine)

#===============With Filter=====================#
minTemps = parsedLines.filter(lambda x: "TMIN" in x[1])
stationTemps = minTemps.map(lambda x: (x[0], x[2]))
#===============Without FIlter==================#
# stationTemps = parsedLines.map(lambda x: (x[0], x[2]))
#===============================================#
minTemps = stationTemps.reduceByKey(lambda x, y: min(x,y))

maxTemps = parsedLines.filter(lambda x: "TMAX" in x[1])
stationTemps = maxTemps.map(lambda x: (x[0], x[2]))
maxTemps = stationTemps.reduceByKey(lambda x, y: max(x,y))

result1 = minTemps.collect()

for r in result1:
    print("\n\n", "station: ",r[0], "\nmin_temp: ", r[1])

result2 = maxTemps.collect()

for r in result2:
    print("\n\n", "station: ",r[0], "\nmax_temp: ", r[1])