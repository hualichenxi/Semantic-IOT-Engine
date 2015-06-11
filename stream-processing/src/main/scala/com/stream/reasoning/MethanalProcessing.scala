package com.stream.reasoning

import org.apache.spark.streaming.{ Seconds, StreamingContext }
import org.apache.spark.streaming.StreamingContext._
import java.io._
import com.stream.receiver.SocketTextStreamReceiver
import com.stream.rdf.processing.RDF

object MethanalProcessing {

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: RDFProcessing <master> <hostname> <port>\n" +
        "In local mode, <master> should be 'local[n]' with n > 1")
      System.exit(1)
    }
    
    // Create the context with a 1 second batch size
    val ssc = new StreamingContext(args(0), "NetworkWordCount", Seconds(2),
      System.getenv("SPARK_HOME"), StreamingContext.jarOfClass(this.getClass))
    ssc.checkpoint(".")
    // Create a NetworkInputDStream on target ip:port
    val rdfs = ssc.networkStream[String](new SocketTextStreamReceiver(args(1), args(2).toInt))
    val methanal = rdfs.filter(_.contains("HCHO")) 
    //    val rdf = rdfs.flatMap(_.split("####"))
    //    val value = rdf.filter(_.contains("Methanal")).map(x=>RDF.rdfStringToModel(x)).map(RDF.getDoubleValueFromModel _)
    
    //    val valuesToArr = values.glom()
    //    val bodyComfortValue = valuesToArr.map(x => (x(0)._2, x(1)._2, calculateBodyComfort(x)))
    //    val comfortLevel = bodyComfortValue.map(x => (x._1, x._2, x._3, bodyComfortLevel(x._3)))
    
    
//    val win = methanal.map(x=>RDF.rdfStringToModel(x)).map(RDF.getDoubleValueFromModel _).window(Seconds(10))
//    val coal = win.transform(rdd=>rdd.coalesce(1))
    
    val value = methanal.map(x=>RDF.rdfStringToModel(x)).map(RDF.getDoubleValueFromModel _)
    
    methanal.print

    ssc.start()
    ssc.awaitTermination()
  }
}