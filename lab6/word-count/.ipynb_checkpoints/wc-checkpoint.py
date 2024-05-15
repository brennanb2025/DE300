from pyspark import SparkContext, SparkConf

DATA = "./data/*.txt"
OUTPUT_DIR = "counts" # name of the folder

def word_count():
    sc = SparkContext("local","Word count example")
    textFile = sc.textFile(DATA)
    counts = textFile.flatMap(lambda line: line.split(" ")).map(lambda word: (word, 1)).reduceByKey(lambda a, b: a + b)
    filtered_counts = counts.filter(lambda x: x[1] >= 3) # Filter words with count >= 3
    filtered_counts.coalesce(1).saveAsTextFile(OUTPUT_DIR) # Put in one file.
    print("Number of partitions: ", textFile.getNumPartitions())
word_count()
