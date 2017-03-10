package kafka

import java.io.FileWriter
import java.util.Properties

import kafka.javaapi.producer.Producer
import kafka.producer.{KeyedMessage, ProducerConfig}
import scala.io.Source

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

  var currentMessage = 0
  if(!new java.io.File("CurrentMessageNumber.txt").exists) storeRunNumber(0) //check if there is previous run which has created messages, fi not create file
  else for (line <- Source.fromFile("CurrentMessageNumber.txt").getLines()) {currentMessage = line.toInt} //otherwise read previous number

  producer.send(new KeyedMessage[String,String]("jsonMessages","127.0.0.1",genVertexAdd()))
  producer.send(new KeyedMessage[String,String]("jsonMessages","127.0.0.1",genVertexAdd()))
  producer.send(new KeyedMessage[String,String]("jsonMessages","127.0.0.1",genVertexAdd()))
  producer.send(new KeyedMessage[String,String]("jsonMessages","127.0.0.1",genVertexAdd()))
  producer.send(new KeyedMessage[String,String]("jsonMessages","127.0.0.1",genVertexAdd()))
  producer.send(new KeyedMessage[String,String]("jsonMessages","127.0.0.1",genVertexAdd()))
  producer.close
  storeRunNumber(currentMessage) //once the run is over, store the current value so this may be used in the next iteration

  def storeRunNumber(runNumber:Int):Unit={
    val fw = new FileWriter(s"CurrentMessageNumber.txt") //if not create the file and show that we are starting at 0
    try {fw.write(runNumber.toString)}
    finally fw.close()
  }

  def genVertexAdd():String={
    currentMessage+=1

    val msgID = getMessageID()
    val srcID = genSetSrcID()
    val properties = genProperties(2,true)

    s""" {"VertexAdd":{$msgID, $srcID, $properties}}"""

  }
  def genVertexUpdateProperties():String={
    currentMessage+=1

    val msgID = getMessageID()
    val srcID = genSrcID()
    val properties = genProperties(2,true)

    s""" {"VertexUpdateProperties":{$msgID, $srcID, $properties}}"""
  }

  def genVertexRemoval():String={
    currentMessage+=1

    val msgID = getMessageID()
    val srcID=genSetSrcID()
    s""" {"VertexRemoval":{$msgID, $srcID}}"""
  }


  def genEdgeAdd():String={
    currentMessage+=1

    val msgID = getMessageID()
    val srcID = genSetSrcID()
    val dstID = genSetDstID()
    val properties =  genProperties(2,true)
    //s""" {"EdgeAdd":{$srcID, $dstID}}"""
    s""" {"EdgeAdd":{$msgID, $srcID, $dstID, $properties}}"""
  }

  def genEdgeUpdateProperties():String={
    currentMessage+=1

    val msgID = getMessageID()
    val srcID = genSetSrcID()
    val dstID = genSetDstID()
    val properties =  genProperties(2,true)
    //s""" {"EdgeAdd":{$srcID, $dstID}}"""
    s""" {"EdgeUpdateProperties":{$msgID, $srcID, $dstID, $properties}}"""
  }

  def genEdgeRemoval():String={
    currentMessage+=1

    val msgID = getMessageID()
    val srcID=genSetSrcID()
    val dstID = genSetDstID()
    s""" {"EdgeRemoval":{$msgID, $srcID, $dstID}}"""
  }

  def genSetSrcID():String = s""" "srcID":9 """
  def genSetDstID():String = s""" "dstID":10 """
  def genSrcID():String = s""" "srcID":${Random.nextInt(20)} """
  def genDstID():String = s""" "dstID":${Random.nextInt(20)} """

  def getMessageID():String = s""" "messageID":$currentMessage """

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
