package com.sparkTutorial.rdd.nasaApacheWebLogs

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

object SameHostsProblem {

  def main(args: Array[String]) {

    /* "in/nasa_19950701.tsv" file contains 10000 log lines from one of NASA's apache server for July 1st, 1995.
       "in/nasa_19950801.tsv" file contains 10000 log lines for August 1st, 1995
       Create a Spark program to generate a new RDD which contains the hosts which are accessed on BOTH days.
       Save the resulting RDD to "out/nasa_logs_same_hosts.csv" file.

       Example output:
       vagrant.vf.mmc.com
       www-a1.proxy.aol.com
       .....

       Keep in mind, that the original log files contains the following header lines.
       host	logname	time	method	url	response	bytes

       Make sure the head lines are removed in the resulting RDD.
     */
    Logger.getLogger("org").setLevel(Level.OFF)

    val conf = new SparkConf().setAppName("SameHostsNasa").setMaster("local[*]")

    val sc = new SparkContext(conf)

    val augustLogs = sc.textFile("in/nasa_19950701.tsv")

    val julyLogs = sc.textFile("in/nasa_19950801.tsv")

    val intersection = augustLogs
      .map(l => l.split("\t")(0))
      .intersection(julyLogs.map(l => l.split("\t")(0)))

    val cleanedIntersection = intersection.filter(l => !l.startsWith("host"))

    cleanedIntersection.saveAsTextFile("out/common-hosts-nasa")
  }
}
