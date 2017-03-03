package kafka

import java.util.Properties

import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import spray.json._

import scala.util.Random

/**
  * Created by Mirate on 02/03/2017.
  */
object jsonGenerator extends App{

  val props:Properties = new Properties()
  props.put("metadata.broker.list", "localhost:9092")
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("partitioner.class", "kafka.SimplePartitioner")
  props.put("request.required.acks", "1")

  val config:ProducerConfig = new ProducerConfig(props)
  val producer = new Producer[String,String](config)

  //println(genAddVertex()

  val data = new KeyedMessage[String,String]("jsonMessages","127.0.0.1",genAddVertex())
  producer.send(data)
  producer.close

  def genAddVertex():String={

    val srcID = genSrcID
    val properties = genProperties(2)

    s""" {"VertexAdd":{$srcID, $properties}}"""

  }

  def genSrcID():String = s""" "srcID":${Random.nextInt(20)} """
  def genDstID():String = s""" "dstID":${Random.nextInt(20)} """

  def genProperties(numOfProps:Int):String ={
    var properties = "\"properties\":{"
    for(i <- 1 to numOfProps){
      if(i<numOfProps) properties = properties + s""" "property$i":${Random.nextInt()}, """
      else properties = properties + s""" "property$i":${Random.nextInt()} }"""
    }
    properties
  }

}
