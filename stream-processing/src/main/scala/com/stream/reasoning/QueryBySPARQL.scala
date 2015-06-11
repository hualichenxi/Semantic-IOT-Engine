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

object QueryBySPARQL {

  /**
   * args(0) <master>
   * args(1) <rdfserver_ip>
   * args(2) <rdfserver_port>
   * args(3) <sparql_ip>
   * args(4) <sparql_port>
   */
  def main(args: Array[String]) {
    if (args.length < 5) {
      System.err.println("Usage: AQIProcessing <master> <server_ip> <server_port> <sparql_ip> <sparql_port>\n" +
        "In local mode, <master> should be 'local[n]' with n > 1")
      System.exit(1)
    }

    PropertyConfigurator.configure(System.getenv("SPARK_HOME") + "/conf/log4j.properties")

    val conf = new SparkConf()
      .setMaster(args(0))
      .setAppName("Query by SPARQL")
      .setSparkHome(System.getenv("SPARK_HOME"))
      .setJars(StreamingContext.jarOfClass(this.getClass))
    //.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    //.set("spark.cleaner.ttl", "10")

    //if run job in standalone cluster mode...
    if (!args(0).contains("local")) {
      conf.set("spark.executor.memory", "1g")
    }

    // Create the context with a 5 second batch size
    val ssc = new StreamingContext(conf, Seconds(5))

    // Create a NetworkInputDStream on target ip:port
    val rdf = ssc.networkStream[String](new SocketTextStreamReceiver(args(1), args(2).toInt))

    val models = rdf.map(RDF.rdfStringToModel _)

    val oneModel = models.reduce((m, n) => {
      m.union(n)
    })

    val query = RDF.getQueryString(args(3), args(4))

    val res = oneModel.map(x => RDF.queryModelByJenaApi(x, query))
    res.print

    ssc.start()
    ssc.awaitTermination(300000)
    ssc.stop()
  }
}