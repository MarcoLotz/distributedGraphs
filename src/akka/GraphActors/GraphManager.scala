package akka.GraphActors

import akka.actor.{Actor, ActorRef, Props}
import spray.json._


/**
  * The Graph Manager is the top level actor in this system (under the stream)
  * which tracks all the graph partitions - passing commands processed by the 'command processor' actors
  * to the correct partition
  */

/**
  * The Command Processor takes string message from Kafka and translates them into
  * the correct case Class which can then be passed to the graph manager
  * which will then pass it to the graph partition dealing with the associated vertex
  */

//The following block are all case classes (commands) which the manager can handle
case class PassPartitionList(partitionList:Map[Int,ActorRef])

case class VertexAdd(srcId:Int) //add a vertex (or add/update a property to an existing vertex)
case class VertexAddWithProperties(srcId:Int, properties: Map[String,String])
case class VertexUpdateProperties(srcId:Int, propery:Map[String,String])
case class VertexRemoval(srcId:Int)

case class EdgeAdd(srcId:Int,destId:Int)
case class EdgeAddWithProperties(srcId:Int,dstId:Int, properties: Map[String,String])
case class EdgeUpdateProperties(srcId:Int,dstId:Int,property:Map[String,String])
case class EdgeRemoval(srcId:Int,dstID:Int)

case class RemoteEdgeUpdateProperties(srcId:Int,dstId:Int,properties:Map[String,String])
case class RemoteEdgeAdd(srcId:Int,dstId:Int)
case class RemoteEdgeAddWithProperties(srcId:Int,dstId:Int,properties: Map[String,String])
case class RemoteEdgeRemoval(srcId:Int,dstId:Int)

class GraphManager() extends Actor{
  var running = false // bool to check if graph has already been initialized
  var childMap = Map[Int,ActorRef]() // map of graph partitions
  var children = 0 // var to store number of children

  //************* MESSAGE HANDLING BLOCK
  override def receive: Receive = {
    case command:PassPartitionList => {childMap = command.partitionList; children = command.partitionList.size}
    case command:String => parseJSON(command)
    case _ => println("message not recognized!")
  }
  def parseJSON(command:String):Unit={
    println(command)
    val parsedOBJ = command.parseJson.asJsObject //get the json object
    val commandKey = parsedOBJ.fields //get the command type

    if(commandKey.contains("VertexAdd")) vertexAdd(parsedOBJ.getFields("VertexAdd").head.asJsObject)
    else if(commandKey.contains("VertexUpdateProperties")) vertexUpdateProperties(parsedOBJ.getFields("VertexUpdateProperties").head.asJsObject)
    else if(commandKey.contains("VertexRemoval")) vertexRemoval(parsedOBJ.getFields("VertexRemoval").head.asJsObject)
    else if(commandKey.contains("EdgeAdd")) edgeAdd(parsedOBJ.getFields("EdgeAdd").head.asJsObject) //if addVertex, parse to handling function
    else if(commandKey.contains("EdgeUpdateProperties")) edgeUpdateProperties(parsedOBJ.getFields("EdgeUpdateProperties").head.asJsObject)
    else if(commandKey.contains("EdgeRemoval")) edgeRemoval(parsedOBJ.getFields("EdgeRemoval").head.asJsObject)
  }
//************ END MESSAGE HANDLING BLOCK

  def vertexAdd(command:JsObject):Unit = {
    val srcId = command.fields("srcID").toString().toInt //extract the srcID
    if(command.fields.contains("properties")){ //if there are properties within the command
    var properties = Map[String,String]() //create a vertex map
      command.fields("properties").asJsObject.fields.foreach( pair => { //add all of the pairs to the map
        properties = properties updated (pair._1,pair._2.toString())
      })
      childMap(chooseChild(srcId)) ! VertexAddWithProperties(srcId,properties) //send the srcID and properties to the graph manager
    }
    else childMap(chooseChild(srcId)) ! VertexAdd(srcId) //if there are not any properties, just send the srcID
  }

  def vertexUpdateProperties(command:JsObject):Unit={
    val srcId = command.fields("srcID").toString().toInt //extract the srcID
    var properties = Map[String,String]() //create a vertex map
    command.fields("properties").asJsObject.fields.foreach( pair => {properties = properties updated (pair._1,pair._2.toString())})
    childMap(chooseChild(srcId)) ! VertexUpdateProperties(srcId,properties) //send the srcID and properties to the graph parition
  }

  def vertexRemoval(command:JsObject):Unit={
    val srcId = command.fields("srcID").toString().toInt //extract the srcID
    childMap(chooseChild(srcId)) ! VertexRemoval(srcId)
  }

  def edgeAdd(command:JsObject):Unit = {
    val srcId = command.fields("srcID").toString().toInt //extract the srcID
    val dstId = command.fields("dstID").toString().toInt //extract the dstID
    if(command.fields.contains("properties")){ //if there are properties within the command
    var properties = Map[String,String]() //create a vertex map
      command.fields("properties").asJsObject.fields.foreach( pair => { //add all of the pairs to the map
        properties = properties updated (pair._1,pair._2.toString())
      })
      childMap(chooseChild(srcId)) ! EdgeAddWithProperties(srcId,dstId,properties) //send the srcID, dstID and properties to the graph manager
    }
    else childMap(chooseChild(srcId)) ! EdgeAdd(srcId,dstId)
  }

  def edgeUpdateProperties(command:JsObject):Unit={
    val srcId = command.fields("srcID").toString().toInt //extract the srcID
    val dstId = command.fields("dstID").toString().toInt //extract the dstID
    var properties = Map[String,String]() //create a vertex map
    command.fields("properties").asJsObject.fields.foreach( pair => {properties = properties updated (pair._1,pair._2.toString())})
    childMap(chooseChild(srcId)) ! EdgeUpdateProperties(srcId,dstId,properties) //send the srcID, dstID and properties to the graph manager
  }

  def edgeRemoval(command:JsObject):Unit={
    val srcId = command.fields("srcID").toString().toInt //extract the srcID
    val dstId = command.fields("dstID").toString().toInt //extract the dstID
    childMap(chooseChild(srcId)) ! EdgeRemoval(srcId,dstId) //send the srcID, dstID to graph manager
  }

  def chooseChild(srcId:Int):Int = srcId % children //simple srcID hash at the moment

}
