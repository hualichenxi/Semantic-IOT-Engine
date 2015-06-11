package com.stream.reasoning

import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import java.io._
import java.net._
import com.stream.receiver.SocketTextStreamReceiver
import com.stream.rdf.processing.RDF
import org.apache.log4j.PropertyConfigurator
import com.hp.hpl.jena.rdf.model.impl.ResourceImpl

object AQIProcessing {

  def filterAQIRelated(s: String) = {
    s.contains("PM10") || s.contains("PM2p5") || s.contains("O3") || s.contains(">CO<") || s.contains("SO2") || s.contains("NO2")
  }

  /**
   * args(0) <master>
   * args(1) <hostname>
   * args(2) <port>
   * args(3) <room_URI>
   */

  def main(args: Array[String]) {
    if (args.length < 4) {
      System.err.println("Usage: AQIProcessing <master> <hostname> <port> <room_URI>\n" +
        "In local mode, <master> should be 'local[n]' with n > 1")
      System.exit(1)
    }

    PropertyConfigurator.configure(System.getenv("SPARK_HOME") + "/conf/log4j.properties")

    val conf = new SparkConf()
      .setMaster(args(0))
      .setAppName("AQI Processing")
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
    //val ssc = new StreamingContext(args(0), "Air Quality Index", Seconds(2), System.getenv("SPARK_HOME"), StreamingContext.jarOfClass(this.getClass))

    val ssc = new StreamingContext(conf, Seconds(5))

    // Create a NetworkInputDStream on target ip:port
    val rdf = ssc.networkStream[String](new SocketTextStreamReceiver(args(1), args(2).toInt))

    val quotes = "\""
    val specificRoom = rdf.filter(_.contains(quotes + args(3) + quotes))

    val aqiRelated = specificRoom.filter(filterAQIRelated _)

    val model = aqiRelated.map(RDF.rdfStringToModel _)

    //val queryStr = "PREFIX p:<http://hem.org/predicate#> SELECT * WHERE{ <http://hem.org/room#bedroom_1> p:hasSensor ?sensor . }"

    //val bedroom = model.filter(x=>x.containsResource(new ResourceImpl("http://hem.org/room#bedroom_1")))

    val values = model.map(RDF.getDoubleValueFromModel _)

    //value:(PM2p5,83.0)
    val aqi = values.map { case (t, c) => (t, c, AQIEvaluation.evaluate(t, c))}

    val aqiForSort = aqi.map { case (t, c, aqi) => (aqi, t)}

    val sorted = aqiForSort.transform(rdd => rdd.sortByKey(false))

    val typeAsKey = sorted.map { case (aqi, t) => (t, aqi)}

    //    typeAsKey.foreach(rdd=>{
    //      val host="localhost"
    //      val port=30001
    //      val s:Socket = new Socket(host,port)
    //      val ps:PrintStream = new PrintStream(s.getOutputStream())
    //      val out=rdd.take(6).mkString("\n")
    //
    ////      println(out)
    ////      println("--------------------------")
    //      ps.println(out)
    //      ps.println("-------------------------")
    //
    //      s.close()
    //    })


    typeAsKey.print
    //    aqi.print
    //    aqiForSort.print
    //    sorted.print
    //    typeAsKey.print

    //   	typeAsKey.foreach(rdd=>{
    //   	  println(rdd.take(6).mkString("\n"))
    //   	  println("\n------------------------")
    //   	})

    //typeAsKey.print

    ssc.start()
    ssc.awaitTermination(300000)
    ssc.stop()
  }
}