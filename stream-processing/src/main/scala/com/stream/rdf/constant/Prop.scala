package com.stream.rdf.constant

import com.hp.hpl.jena.rdf.model.impl.PropertyImpl

//Properties used in RDF
object Prop {
  val hasValue = new PropertyImpl(Uri4Rdf.predURI+"hasValue")
  val valueType = new PropertyImpl(Uri4Rdf.predURI+"valueType")
  //val generatedBy = new PropertyImpl(Uri4Rdf.predURI+"generatedBy")
  val samplingTime = new PropertyImpl(Uri4Rdf.predURI+"samplingTime")
  //val sensorID = new PropertyImpl(Uri4Rdf.predURI+"sensorID")
  //val locatedAt = new PropertyImpl(Uri4Rdf.predURI+"locatedAt")
}