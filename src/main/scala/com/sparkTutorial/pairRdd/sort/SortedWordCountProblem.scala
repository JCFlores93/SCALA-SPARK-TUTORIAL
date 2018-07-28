package com.sparkTutorial.pairRdd.sort

import org.apache.spark.{SparkConf, SparkContext}


object SortedWordCountProblem {

    /* Create a Spark program to read the an article from in/word_count.text,
       output the number of occurrence of each word in descending order.

       Sample output:

       apple : 200
       shoes : 193
       bag : 176
       ...
     */
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
      .setAppName("sortedWordCountProblem")
      .setMaster("local[3]")
    val sc = new SparkContext(conf)
    val lines = sc.textFile("in/word_count.text")
    val wordRDD = lines.flatMap(line => line.split(" "))
        .map(word => (word,1))
        .reduceByKey((x, y) => x +y)
    val countToWordParis = wordRDD.map(wordToCount => (wordToCount._2, wordToCount._1))
    val sortedCountToWordParis = countToWordParis.sortByKey(ascending = false)
        .map(countToWord => (countToWord._2, countToWord._1))

    for ((word, count) <- sortedCountToWordParis.collect()) println(word + " : " + count)

  }


}

