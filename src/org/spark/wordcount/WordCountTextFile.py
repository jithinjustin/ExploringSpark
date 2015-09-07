__author__ = 'jithinjustin'
import sys
sys.path.append("/Users/jithinjustin/spark-1.4.1-bin-hadoop2.6/python/")
from operator import add
from pyspark import SparkContext
import re


def wordCount(wordListRDD):
    wordCountRDD=wordListRDD.map(lambda x:(x,1)).reduceByKey(add)
    return wordCountRDD


def removePunctuation(text):
    return re.sub(r'[^a-z0-9\s]','',text.lower()).strip()

if __name__ == '__main__':

    sc=SparkContext()
    shakespeareRDD = (sc
                  .textFile("shakesphere.txt", 8)
                  .map(removePunctuation))
    shakespeareRDD.cache()
    #zip index with line
    print '\n'.join(shakespeareRDD
                .zipWithIndex()  # to (line, lineNum)
                .map(lambda (l, num): '{0}: {1}'.format(num, l))  # to 'lineNum: line'
                .take(15))

    #convert the sentences to individual words
    shakespeareWordsRDD = shakespeareRDD.flatMap(lambda x: x.split())
    shakespeareWordCount = shakespeareWordsRDD.count()
    print shakespeareWordsRDD.top(5)
    print shakespeareWordCount

    #prune out empty words
    shakeWordsRDD = shakespeareWordsRDD.filter(lambda x : (x!=''))
    shakeWordCount = shakeWordsRDD.count()
    print shakeWordCount


    #give top 20 words with count
    top20WordsAndCounts = wordCount(shakeWordsRDD).takeOrdered(20, lambda (k, v): -v)
    print '\n'.join(map(lambda (w, c): '{0}: {1}'.format(w, c), top20WordsAndCounts))

