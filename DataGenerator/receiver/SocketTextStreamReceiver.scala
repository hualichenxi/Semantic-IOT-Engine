package com.stream.receiver

import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.storage.StorageLevel
import java.io._
import java.net.Socket
import org.apache.spark.streaming.dstream.NetworkReceiver

class SocketTextStreamReceiver(host: String, port: Int) extends NetworkReceiver[String] {
  protected lazy val blocksGenerator: BlockGenerator = new BlockGenerator(StorageLevel.MEMORY_ONLY_SER_2)

  protected def onStart() = {
    blocksGenerator.start()
    val socket = new Socket(host, port)
    val dataInputStream = new BufferedReader(new InputStreamReader(socket.getInputStream(), "UTF-8"))
    
    var line: String = dataInputStream.readLine()
    var total = new StringBuffer("")
    while (line != null) {
      total.append(line)
      if (line == "</rdf:RDF>") {
        blocksGenerator += total.toString
        total = new StringBuffer("")
      }
      line = dataInputStream.readLine()
    }
  }

  protected def onStop() {
    blocksGenerator.stop()
  }
}