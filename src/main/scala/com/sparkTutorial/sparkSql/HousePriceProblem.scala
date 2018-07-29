package com.sparkTutorial.sparkSql

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession


object HousePriceProblem {

        /* Create a Spark program to read the house data from in/RealEstate.csv,
           group by location, aggregate the average price per SQ Ft and sort by average price per SQ Ft.

        The houses dataset contains a collection of recent real estate listings in San Luis Obispo county and
        around it. 

        The dataset contains the following fields:
        1. MLS: Multiple listing service number for the house (unique ID).
        2. Location: city/town where the house is located. Most locations are in San Luis Obispo county and
        northern Santa Barbara county (Santa Maria­Orcutt, Lompoc, Guadelupe, Los Alamos), but there
        some out of area locations as well.
        3. Price: the most recent listing price of the house (in dollars).
        4. Bedrooms: number of bedrooms.
        5. Bathrooms: number of bathrooms.
        6. Size: size of the house in square feet.
        7. Price/SQ.ft: price of the house per square foot.
        8. Status: type of sale. Thee types are represented in the dataset: Short Sale, Foreclosure and Regular.

        Each field is comma separated.

        Sample output:

        +----------------+-----------------+
        |        Location| avg(Price SQ Ft)|
        +----------------+-----------------+
        |          Oceano|             95.0|
        |         Bradley|            206.0|
        | San Luis Obispo|            359.0|
        |      Santa Ynez|            491.4|
        |         Cayucos|            887.0|
        |................|.................|
        |................|.................|
        |................|.................|
         */
        val AGE_MIDPOINT = "age_midpoint"
        val SALARY_MIDPOINT = "salary_midpoint"
        val SALARY_MIDPOINT_BUCKET = "salary_midpoint_bucket"

        def main(args: Array[String]): Unit = {
                Logger.getLogger("org").setLevel(Level.ERROR)
                val session = SparkSession.builder().appName("HousePriceProblem").master("local[1]").getOrCreate()

                val dataFrameReader = session.read

                val responses = dataFrameReader
                  .option("header", "true")
                  .option("inferSchema", value = true)
                  .csv("in/RealEstate.csv")

                System.out.println("=====Print out schema====")
                responses.printSchema()

                val responseWithSelectedColumns = responses.select("Location","Price SQ Ft")
                System.out.println("=== Print the selected columns of the table ===")
                responseWithSelectedColumns.show()

                System.out.println("=== Print the selected columns of the table ===")
                val responsesGroupedByLocation = responseWithSelectedColumns
                  .groupBy("Location")
                  .avg("Price SQ Ft")
                  .orderBy("avg(Price SQ Ft)")
                  .show(100)
                //group by location

        }


}
