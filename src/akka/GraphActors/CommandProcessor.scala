package akka.GraphActors

import akka.actor.{Actor, ActorRef}
import spray.json._


/**
  * The Command Processor takes string message from Kafka and translates them into
  * the correct case Class which can then be passed to the graph manager
  * which will then pass it to the graph partition dealing with the associated vertex
  *
  * Currently only have Add vertex implemented, but others will be added over time
  * Also plan to swap this from basic strings to XML or JSON
  * As current commands are very limited and really splitting on space is poor practice
  */

class CommandProcessor(graphManager: ActorRef) extends Actor{ //New Command Processors are given reference to the graph manager
  override def receive: Receive = {
    case command:String => parseJSON(command)
  }

  def parseJSON(command:String):Unit={
    println(command)
    val parsedOBJ = command.parseJson.asJsObject //get the json object
    val commandKey = parsedOBJ.fields //get the command type
    if(commandKey.contains("VertexAdd")) {
      vertexAdd(parsedOBJ.getFields("VertexAdd").head.asJsObject) //if addVertex, parse to handling function
    }
    else if(commandKey.contains("VertexUpdateProperties")) {
      vertexUpdateProperties(parsedOBJ.getFields("VertexUpdateProperties").head.asJsObject)
    }

    else if(commandKey.contains("EdgeAdd")) {
      edgeAdd(parsedOBJ.getFields("EdgeAdd").head.asJsObject) //if addVertex, parse to handling function
    }
  }

  def vertexAdd(command:JsObject):Unit = {
    val srcID = command.fields("srcID").toString().toInt //extract the srcID
    if(command.fields.contains("properties")){ //if there are properties within the command
      var properties = Map[String,String]() //create a vertex map
      command.fields("properties").asJsObject.fields.foreach( pair => { //add all of the pairs to the map
        properties = properties updated (pair._1,pair._2.toString())
      })
      graphManager ! VertexAddWithProperties(srcID,properties) //send the srcID and properties to the graph manager
    }
    else graphManager ! VertexAdd(srcID) //if there are not any properties, just send the srcID
  }

  def vertexUpdateProperties(command:JsObject):Unit={
    val srcID = command.fields("srcID").toString().toInt //extract the srcID
    var properties = Map[String,String]() //create a vertex map
    command.fields("properties").asJsObject.fields.foreach( pair => {properties = properties updated (pair._1,pair._2.toString())})
    graphManager ! VertexUpdateProperties(srcID,properties) //send the srcID and properties to the graph manager
  }


  def edgeAdd(command:JsObject):Unit = {
    val srcID = command.fields("srcID").toString().toInt //extract the srcID
    val dstID = command.fields("dstID").toString().toInt //extract the dstID
    if(command.fields.contains("properties")){ //if there are properties within the command
    var properties = Map[String,String]() //create a vertex map
      command.fields("properties").asJsObject.fields.foreach( pair => { //add all of the pairs to the map
        properties = properties updated (pair._1,pair._2.toString())
      })
      graphManager ! EdgeAddWithProperties(srcID,dstID,properties) //send the srcID, dstID and properties to the graph manager
    }
    else graphManager ! EdgeAdd(srcID,dstID)
  }


}
