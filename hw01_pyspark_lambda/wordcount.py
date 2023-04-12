import findspark

findspark.init()

from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Hw01App")
sc = SparkContext(conf=conf)

sc.setLogLevel("ERROR")

lines_rdd = sc.textFile("TheTimeMachine.txt")

time_traveler_upper = lines_rdd.filter(lambda x: "Time Traveler" in x).collect()
print(f"Time Traveler count: {len(time_traveler_upper)}")

time_traveler_lower = lines_rdd.filter(lambda x: "time traveler" in x).collect()
print(f"time traveler count: {len(time_traveler_lower)}")

clambering = lines_rdd.filter(lambda x: "clambering" in x).collect()
print(f"clambering count: {len(clambering)}")

morlocks = lines_rdd.filter(lambda x: "Morlocks" in x).collect()
print(f"Morlocks: {len(morlocks)}")
