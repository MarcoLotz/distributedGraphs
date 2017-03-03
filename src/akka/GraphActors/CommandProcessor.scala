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
    val parsedOBJ = command.parseJson.asJsObject //get the json object
    val commandKey = parsedOBJ.fields //get the command type
    if(commandKey.contains("VertexAdd")) {
      vertexAdd(parsedOBJ.getFields("VertexAdd").head.asJsObject) //if addVertex, parse to handling function
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

  //graphManager ! VertexAddProperty(srcID,(pair._1,pair._2.toString()))


  def edgeAdd(split:Array[String]):Unit = {
    graphManager ! EdgeAdd(split(1).toInt,split(2).toInt,(split(3),split(4)))
  }


//val split = command.split(" ") //split the incoming strings on space

  //if(split(0).equals("addV")) vertexAdd(split) //if the command is for adding a vertex
 // else if (split(0).equals("addE")) edgeAdd(split) //if the command is for adding an edge
  //else println("Command not understood") // else case for command not understood


}
