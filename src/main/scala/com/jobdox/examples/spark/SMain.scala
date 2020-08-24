package com.jobdox.examples.spark

import org.apache.log4j.Logger
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.SparkSession

object SMain extends App{
  import org.apache.log4j.BasicConfigurator
  BasicConfigurator.configure()
  val logger = Logger.getLogger(SMain.getClass)

  logger.info("This is my first log4j's statement")

  val spark = SparkSession
    .builder
    .master("local[5]")
    .appName("Inmemorytest")
    .getOrCreate()

  import spark.implicits._

  val conf = new SparkConf().setAppName("sparkbyexamples.com").setMaster("local[8]")
  val sc = spark.sparkContext // new SparkContext(conf)
  sc.setLogLevel("ERROR")

  val data = Array(1, 2, 3, 4, 5)
  val rdd = sc.parallelize(data)
  val partitionCount = rdd.getNumPartitions
  val rdd2 = rdd.repartition(2)
  val partitionCount2 = rdd2.getNumPartitions
  println(s"partitionCount: $partitionCount, partitionCount2: $partitionCount2")

  val employeePath = Util.getClassFilePath("employee.csv")
  val payrollPath = Util.getClassFilePath("payroll.csv")
  val df = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(employeePath)
  df.printSchema()
  val df2 = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(employeePath)
  val dfPayroll = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(payrollPath)

  val dfJoined = df.join(df2, df("mgrid") === df2("id"), "inner")
  println("Employee/Manager Join")
  dfJoined.collect().foreach(r => println(r))
  val dfJoined2 = df.join(dfPayroll, df("id") === dfPayroll("id"), "leftsemi")
  println()
  println("Employee/Payroll Join")
  dfJoined2.collect().foreach(r => println(r))


  sc.stop()

}
