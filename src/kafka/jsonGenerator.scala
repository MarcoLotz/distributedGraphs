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

  var messageID:Int = 0

  val props:Properties = new Properties()
  props.put("metadata.broker.list", "localhost:9092")
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("partitioner.class", "kafka.SimplePartitioner")
  props.put("request.required.acks", "1")

  val config:ProducerConfig = new ProducerConfig(props)
  val producer = new Producer[String,String](config)

  producer.send(new KeyedMessage[String,String]("jsonMessages","127.0.0.1",genEdgeUpdateProperties()))
  producer.close

  def genVertexAdd():String={

    val srcID = genSrcID
    val properties = genProperties(2,false)

    s""" {"VertexAdd":{$srcID, $properties}}"""

  }
  def genVertexUpdateProperties():String={
    val srcID = genSrcID()
    val properties = genProperties(2,true)

    s""" {"VertexUpdateProperties":{$srcID, $properties}}"""
  }

  def genEdgeAdd():String={
    val srcID = genSetSrcID()
    val dstID = genSetDstID()
    val properties =  genProperties(2,false)
    //s""" {"EdgeAdd":{$srcID, $dstID}}"""
    s""" {"EdgeAdd":{$srcID, $dstID, $properties}}"""
  }

  def genEdgeUpdateProperties():String={
    val srcID = genSetSrcID()
    val dstID = genSetDstID()
    val properties =  genProperties(2,false)
    //s""" {"EdgeAdd":{$srcID, $dstID}}"""
    s""" {"EdgeUpdateProperties":{$srcID, $dstID, $properties}}"""
  }

  def genSetSrcID():String = s""" "srcID":9 """
  def genSetDstID():String = s""" "dstID":10 """
  def genSrcID():String = s""" "srcID":${Random.nextInt(20)} """
  def genDstID():String = s""" "dstID":${Random.nextInt(20)} """

  def genMessageID():String = s""" "messageID":$messageID """

  def genProperties(numOfProps:Int,randomProps:Boolean):String ={
    var properties = "\"properties\":{"
    for(i <- 1 to numOfProps){
      val propnum = {if(randomProps) Random.nextInt(20) else i}
      if(i<numOfProps) properties = properties + s""" "property$propnum":${Random.nextInt()}, """
      else properties = properties + s""" "property$i":${Random.nextInt()} }"""
    }
    properties
  }

}
