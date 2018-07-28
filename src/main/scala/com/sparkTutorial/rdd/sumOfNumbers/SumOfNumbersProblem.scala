package com.sparkTutorial.rdd.sumOfNumbers

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SumOfNumbersProblem {

  def main(args: Array[String]) {

    /* Create a Spark program to read the first 100 prime numbers from in/prime_nums.text,
       print the sum of those numbers to console.

       Each row of the input file contains 10 prime numbers separated by spaces.
     */
    Logger.getLogger("org").setLevel(Level.OFF)
    val conf = new SparkConf().setAppName("reduce").setMaster("local[*]")
    val sc = new SparkContext(conf)

    val airports = sc.textFile("in/prime_nums.text")
    val integerRDD = sc.parallelize(airports.collect())
    print(s" resultado ${integerRDD.take(100).reduce((a, b) => a + b)}")
    airports.foreach(println)
    print("------------------------------------------")
    integerRDD.foreach(println)
    //println(integerRDD)
  }
}
