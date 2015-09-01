#!/Users/rt/spark-1.3.1/bin/pyspark

import os
import sys

"""

"""
def main():
    os.environ['SPARK_HOME'] = "/Users/rt/spark-1.3.1"
    sys.path.append("/Users/rt/spark-1.3.1/python")
    sys.path.append("/Users/rt/spark-1.3.1/python/lib/py4j-0.8.2.1-src.zip")

    try:
        import pyspark

    except ImportError as e:
        print ("Error importing Spark Modules", e)
        sys.exit(1)

    conf = pyspark.SparkConf()
    sc = pyspark.SparkContext()
    lines = sc.textFile('textfileexample')
    lines_nonempty = lines.filter( lambda x: len(x) > 0 )
    line_count = lines_nonempty.count()
    print "\n"
    print "This file contains: " + str(line_count) + " lines."
    words = lines_nonempty.flatMap(lambda x: x.split())
    wc_rdd = words.map(lambda x: (x, 1)).reduceByKey(lambda x, y: x+y).map(lambda x: (x[1], x[0])).sortByKey(False)
    word_count = wc_rdd.count()
    print "This file contains: " + str(word_count) + " words."

if __name__ == '__main__':
    main()

