package com.stream.rdf.processing

import com.hp.hpl.jena.rdf.model._
import java.io._
import java.net._
import com.hp.hpl.jena.rdf.model.impl.PropertyImpl
import com.hp.hpl.jena.query._
import com.stream.rdf.constant.Prop
import com.stream.rdf.constant.Uri4Rdf

object RDF {
  def rdfStringToModel(str: String): Model = {
    val in = new ByteArrayInputStream(str.getBytes("UTF-8"))
    val model: Model = ModelFactory.createDefaultModel()
    model.read(in, "")
    in.close()
    model
  }
  
  def getQueryString(ip:String,port:String):String = {
    val s = new Socket(ip,port.toInt)
    val br = new BufferedReader(new InputStreamReader(s.getInputStream()))
    br.readLine()
  }
  
//  def isContains(uri:String, model:Model):Boolean = {
////    val query:Query = QueryFactory.create(queryStr)
////    val qe:QueryExecution = QueryExecutionFactory.create(query,model)
////    val rs:ResultSet = qe.execSelect()
////    rs.hasNext()
//    //model.containsResource(new ResourceImpl(uri))
//    
//  }
  
  def queryModelByJenaApi(model:Model,queryStr:String):String = {
    val query = QueryFactory.create(queryStr)
    val qe = QueryExecutionFactory.create(query,model)
    val rs = qe.execSelect()
    val re = ResultSetFormatter.asText(rs)
    qe.close()
    re
  }
  
  def getType(m:Model):String = {
    val itr:StmtIterator=m.listStatements(null,Prop.valueType,null) 
    itr.next().getString()
  }
  
  def getDoubleValueFromModel(m: Model): (String, Double) = {
    val itr = m.listResourcesWithProperty(Prop.valueType)

    val res=itr.nextResource()
    
    
    val ty = res.getRequiredProperty(Prop.valueType).getString
    val value = res.getRequiredProperty(Prop.hasValue).getString
    //val sensor = model.getResource(Uri4Rdf.obsURI).getRequiredProperty(Prop.generatedBy).getResource()
    //val sensor_id = sensor.getRequiredProperty(Prop.sensorID).getString
    //val location = sensor.getRequiredProperty(Prop.locatedAt).getString
    //val time = model.getResource(Uri4Rdf.obsURI).getRequiredProperty(Prop.samplingTime).getString
    //ty + ":" + value+"##"+"generatedBy:"+sensor_id+"##"+"location:"+location+"##"+"sampingTime:"+time
    return (ty, value.toDouble)
  }
  
  def getIntValueFromModel(model:Model):(String, Int)={
    val ty = model.getResource(Uri4Rdf.sensorURI).getRequiredProperty(Prop.valueType).getString
    val value = model.getResource(Uri4Rdf.sensorURI).getRequiredProperty(Prop.hasValue).getString
//    val sensor = model.getResource(Uri4Rdf.obsURI).getRequiredProperty(Prop.generatedBy).getResource()
//    val sensor_id = sensor.getRequiredProperty(Prop.sensorID).getString
//    val location = sensor.getRequiredProperty(Prop.locatedAt).getString
//    val time = model.getResource(Uri4Rdf.obsURI).getRequiredProperty(Prop.samplingTime).getString
    //ty + ":" + value+"##"+"generatedBy:"+sensor_id+"##"+"location:"+location+"##"+"sampingTime:"+time
    return (ty, value.toInt)
  }
}