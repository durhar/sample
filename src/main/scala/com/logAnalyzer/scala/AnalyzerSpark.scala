package com.logAnalyzer.scala

import com.logAnalyzer.scala.AnalyzerTransformer.logger

import java.io.File
import java.text.SimpleDateFormat
import java.util.Calendar
import scala.collection.JavaConverters
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.dense_rank
import org.apache.spark.sql.functions.not
import com.typesafe.config.ConfigFactory
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.Row
import org.apache.spark.sql.Dataset
import org.apache.log4j.{Level, Logger}

import java.net.URL
import java.net.HttpURLConnection
import java.io.InputStream
import java.io.OutputStream
import java.io.BufferedOutputStream
import java.io.FileOutputStream
import java.io.IOException
import scala.Console.in



case class AccessLog(host: String, date: String, method: String, endpoint: String, responseCode: String)
case class LogAnalysisException(severity: String, message: String, functionName: String) extends Exception(message: String)

object AnalyzerSpark {

  import org.apache.log4j.BasicConfigurator

  BasicConfigurator.configure()

  val logger = Logger.getLogger(this.getClass.getName())
  var configFilePath: String = ""
  var resultFileLoc: String = ""

  def main(args: Array[String]): Unit = {
    val logger = Logger.getLogger(this.getClass.getName())
    val conf = new SparkConf()
      .setAppName("Analyse-NASA-Log")
      .setMaster("local")




    //Parse command-line-arguments
    parseInputArgs(args)
    logger.info("Parsed command-line-arguments.")

    //Read config file and set configuration properties
    val configfile = ConfigFactory.parseFile(new File(configFilePath))
    logger.info("Parsed config file- " + configFilePath)

    val numOfResultsToFetch = configfile.getInt("analyzer.valueOfN.value")
    val writeCurruptLogLines = configfile.getBoolean("analyzer.writeCurruptLogEnteries.value")
    val filterResponseCodes = configfile.getStringList("analyzer.filterResponseCodes.value")
    val filterResponseCodesSeq = JavaConverters.asScalaIteratorConverter(filterResponseCodes.iterator())
      .asScala.toSeq
    //blank value of downloadURL means file is already present. This setting is for re-run approach or incase docker image is not able to connect outside.
    val downloadURL = configfile.getString("analyzer.downloadFileLoc.value")
    val fileLocation = configfile.getString("analyzer.gzFSLocation.value")

    if (numOfResultsToFetch <= 0) {
      logger.error("The number of results to fetch is not defined in property file")
      System.exit(0);
    }
    if (fileLocation.length() <= 0) {
      logger.error("The file system directory to keep downloaded file is not defined in property file")
      System.exit(0);
    }

    if (!downloadURL.equals("")) {
      if (getFile(downloadURL, fileLocation) == 0) {
        logger.error("Exception : Exiting execution to error while downloading file.")
        System.exit(0)
      }
      logger.info("Download complete at location- " + fileLocation)
    } else {
      logger.info("Download not required. File is already at location- " + fileLocation)
    }

    logger.info("Parsed command-line-arguments.")


    val sparkSession = SparkSession.builder().config(conf).getOrCreate()
    val sc = sparkSession.sparkContext
    val startTime = System.currentTimeMillis()
    //val dataSource = "ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz"


    //sc.addFile(dataSource)

    /* val fileName = SparkFiles.get(dataSource.split("/").last)
   val file = sc.textFile(fileName)
   println("Hello this is Durga"+file)*/


    val dataRdd = sc.textFile("C:/files/NASA_access_log_Jul95.gz")
   // dataRdd.toArray().foreach(line => println(line))
    // println("IAm here")
    // println(dataRdd.getClass)



    import sparkSession.implicits._
    logger.info("Start Transformation... ")
    val logLines = dataRdd.map(_.split(" ")).map(attributes => (attributes.length, attributes)).toDS()
    logLines.cache()
    //Filter logs based on tokens.size, i.e. attribute.length.
    val filteredLogLinesWithHttp = logLines.filter(logLines("_1") === 10)
    val filteredLogLinesWithoutHttp = logLines.filter(logLines("_1") === 9)
    filteredLogLinesWithHttp.printSchema()
    filteredLogLinesWithHttp.show()
    filteredLogLinesWithoutHttp.printSchema()
    filteredLogLinesWithoutHttp.show()
    //Filter corrupt logLines entries with incomplete information for processing.
    if (writeCurruptLogLines) {
      val curruptLogLines = logLines.filter(logLines("_1") < 9)
      val curruptLogLinesFormatted = curruptLogLines.map(x => x._1 + " - " + (x._2).mkString(" "))
      curruptLogLinesFormatted.printSchema()
      curruptLogLinesFormatted.show()

      AnalyzerTransformer.writeCurruptLogLinesToFS(curruptLogLinesFormatted, resultFileLoc)
    }

    logLines.unpersist()

    //Create datasets - AccessLog with & without HTTP
    val accessLogLinesWithHttp = filteredLogLinesWithHttp.map(x =>
      AccessLog(
        x._2(0),
        x._2(3).substring(1, x._2(3).indexOf(":")),
        x._2(5).substring(1),
        x._2(6).trim,
        x._2(8).trim))
    val accessLogLinesWithoutHttp = filteredLogLinesWithoutHttp.map(x =>
      AccessLog(
        x._2(0),
        x._2(3).substring(1, x._2(3).indexOf(":")),
        x._2(5).substring(1),
        x._2(6).substring(0, x._2(6).length - 1),
        x._2(7).trim))
    val accessLogLines = accessLogLinesWithoutHttp.union(accessLogLinesWithHttp)
    accessLogLines.cache()
    logger.info("AccessLog lines prepared..")

    //Define window for ranking operation
    val window = Window.partitionBy($"date").orderBy($"count".desc)

    //Fetch topN visitors
    val accessLogLinesTopVisitors = AnalyzerTransformer.getTopNVisitors(accessLogLines, numOfResultsToFetch, window)
    logger.info("Top" + numOfResultsToFetch + " Visitors processed.")

    //Fetch topN urls
    val accessLogLinesTopUrls = AnalyzerTransformer.getTopNUrls(accessLogLines, numOfResultsToFetch, window, filterResponseCodesSeq)
    logger.info("Top" + numOfResultsToFetch + " urls processed.")

    accessLogLines.unpersist()

    //Write the output to FS
    AnalyzerTransformer.writeResultsToFS(accessLogLinesTopVisitors, accessLogLinesTopUrls, resultFileLoc, numOfResultsToFetch)
    logger.info("Data processing finished. Output location- " + resultFileLoc)

    sc.stop()
  }

  //Function to parse command-line arguments and set local variables
  def parseInputArgs(args: Array[String]) = {
    if (args.length < 2) {
      logger.error("The application property file path and/or output directory path are missing in the argument list!")
      System.exit(0);
    } else {
      configFilePath = args(0)
      resultFileLoc = args(1)
    }
  }

  //Function to download the file from ftp
  def getFile(downloadURL: String, filePath: String): Int = {
    val url = new URL(downloadURL)
    var return_code = 1
    var in: InputStream = null
    var out: OutputStream = null

    // val data = sparkSession.sparkContext.textFile("ftp://ita.ee.lbl.gov/traces/NASA_access_log_Jul95.gz", 4).toJavaRDD.map((x: String) => Uttils.getField(x))

    try {
      val connection = url.openConnection()
      connection.setConnectTimeout(5000)
      connection.setReadTimeout(5000)
      connection.connect()
      logger.info("Connection to FTP server successfull.")

      in = connection.getInputStream
     val fileToDownloadAs = new java.io.File(filePath)
     out = new BufferedOutputStream(new FileOutputStream(fileToDownloadAs))
      logger.info("Downloading file from location- " + downloadURL)
      logger.info("Downloading file to location- " + filePath)

      val byteArray = Stream.continually(in.read).takeWhile(-1 !=).map(_.toByte).toArray
      out.write(byteArray)

    } catch {
      case ex: Exception =>
      {
        logger.error(new LogAnalysisException("error", s"getFile from ftp failed. Msg: $ex", "getFile").toString());
        return_code = 0
      }

    } finally {
      try {
        in.close()
        out.close()
      } catch {
        case ex: Exception => logger.error(new LogAnalysisException("error", s"Closing input/output streams. Msg: $ex", "getFile").toString());
      }

    }
     return_code
  }

  }



