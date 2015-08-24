__author__ = 'jithinjustin'

import os
import sys

from pyspark import SparkContext
from pyspark import SparkConf
from operator import add

sc=SparkContext()
wordsList = ['cat', 'elephant', 'rat', 'rat', 'cat']
print "wordlist"
print wordsList
wordsRDD = sc.parallelize(wordsList, 4)
# Print out the type of wordsRDD
print type(wordsRDD)
wordPairs = wordsRDD.map(lambda x: (x,1))
wordCounts = wordPairs.reduceByKey(add)
print "wordpairs "
print wordPairs.collect()
print "wordcounts "
print wordCounts.collect()

wordCountsCollected = (wordsRDD.map(lambda x:(x,1)).reduceByKey(add)
                       .collect())
print "collected wordcount as a single line of code"
print wordCountsCollected

uniqueWords = len(wordCountsCollected)
print "unique words ",uniqueWords

totalCount = (wordCounts
              .map(lambda (k,v): v)
              .reduce(lambda a,b: a + b))

average = totalCount / float(uniqueWords)
print "total count",totalCount
print "average per unique word",round(average, 2)