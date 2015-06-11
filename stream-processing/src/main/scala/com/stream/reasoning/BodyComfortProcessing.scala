package com.stream.reasoning

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkConf
import java.io._
import java.math.BigDecimal
import com.stream.receiver.SocketTextStreamReceiver
import com.stream.rdf.processing.RDF
import org.apache.log4j.PropertyConfigurator

object BodyComfortProcessing {
  /**
   * 函数：计算人体舒适度指数
   * t 平均温度，用摄氏度表示
   * h 相对湿度，如56.3%，应该传入56.3
   * v 风速
   * ssd=(1.818t+18.18)(0.88+0.002h)+(t-32)/(45-t)-3.2v+18.2
   */
  def bodyComfort(t: Double, h: Double, v: Double) = {
    val ssd = (1.818 * t + 18.18) * (0.88 + 0.002 * (h / 100)) + (t - 32) / (45 - t) - 3.2 * v + 18.2
    val bd = new BigDecimal(ssd)
    bd.setScale(0, BigDecimal.ROUND_HALF_UP).intValue()
  }

  //  def calculateBodyComfort(arr: Array[(String, Double)]) = {
  //    var temp = 0.0
  //    var humidity = 0.0
  //    for ((k, v) <- arr) {
  //      if (k == "temp") { temp = v }
  //      if (k == "humidity") { humidity = v }
  //    }
  //    bodyComfort(temp, humidity, 0)
  //  }

  /**
   * ---人体舒适度指数分级---
   * 86—88，4级 	人体感觉很热，极不适应，注意防暑降温，以防中暑；
   * 80—85，3级 	人体感觉炎热，很不舒适，注意防暑降温；
   * 76—79，2级 	人体感觉偏热，不舒适，可适当降温；
   * 71—75，1级 	人体感觉偏暖，较为舒适；
   * 59—70，0级 	人体感觉最为舒适，最可接受；
   * 51—58，-1级 人体感觉略偏凉，较为舒适；
   * 39—50，-2级 人体感觉较冷(清凉)，不舒适，请注意保暖；
   * 26—38，-3级 人体感觉很冷，很不舒适，注意保暖防寒；
   * <25，-4级 	人体感觉寒冷，极不适应，注意保暖防寒，防止冻伤。
   */
  def humanComfortLevel(v: Int) = {
    if (v > 88) 5
    else if (86 <= v && v <= 88) 4
    else if (80 <= v && v <= 85) 3
    else if (76 <= v && v <= 79) 2
    else if (71 <= v && v <= 75) 1
    else if (59 <= v && v <= 70) 0
    else if (51 <= v && v <= 58) -1
    else if (39 <= v && v <= 50) -2
    else if (26 <= v && v <= 38) -3
    else -4
  }

  def humanComfortIndex(x: (String, Double), y: (String, Double)): (String, Double) = {
    var temp = 0.0
    var humidity = 0.0
    if (x._1 == "temp") {
      temp = x._2
      humidity = y._2
    } else {
      temp = y._2
      humidity = x._2
    }
    return ("comfort", bodyComfort(temp, humidity, 0).toDouble)
  }

  def filterTempAndHumidity(str: String) = {
    str.contains("temp") || str.contains("humidity")
  }

  /**
   * args(0) <master>
   * args(1) <hostname>
   * args(2) <port>
   * args(3) <room_URI>
   */
  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: BodyComfortProcessing <master> <hostname> <port> <room_URI>\n" +
        "In local mode, <master> should be 'local[n]' with n > 1")
      System.exit(1)
    }

    PropertyConfigurator.configure(System.getenv("SPARK_HOME") + "/conf/log4j.properties")

    // Create the context with a 5 second batch size
    val conf = new SparkConf()
      .setMaster(args(0))
      .setAppName("Body Comfort")
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(StreamingContext.jarOfClass(this.getClass))
    //.setJars(List("/home/spark/biyesheji/stream-processing/out/artifacts/stream_processing_jar/stream-processing.jar"))
    //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //.set("spark.cleaner.ttl", "10")

    //run job in standalone cluster mode...
    if (!args(0).contains("local")) {
      conf.set("spark.executor.memory", "1g")
    }

    // Create the context with a 5 second batch size
    val ssc = new StreamingContext(conf, Seconds(5))

    // Create a NetworkInputDStream on target ip:port
    val rdf = ssc.networkStream[String](new SocketTextStreamReceiver(args(1), args(2).toInt))

    val quotes = "\""
    val room = rdf.filter(_.contains(quotes + args(3) + quotes))

    val comfortRelated = room.filter(filterTempAndHumidity _)

    val model = comfortRelated.map(RDF.rdfStringToModel _)

    val values = model.map(RDF.getDoubleValueFromModel _)

    val comfortIndex = values.reduce(humanComfortIndex)

    val comfortLevel = comfortIndex.map(x => (x._2, humanComfortLevel(x._2.toInt)))

    //values.print
    //comfortIndex.print
    comfortLevel.print

    ssc.start()
    ssc.awaitTermination()
  }
}