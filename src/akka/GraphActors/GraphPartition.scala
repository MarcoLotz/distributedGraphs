package akka.GraphActors

import akka.GraphEntities.{Edge, RemoteEdge, RemotePos, Vertex}
import akka.actor.{Actor, ActorRef}
import java.io._
/**
  * The graph partition manages a set of vertices and there edges
  * Is sent commands which have been processed by the command Processor
  * Will process these, storing information in graph entities which may be updated if they already exist
  *
  * */

//need to finish up remove edge and do remote and beyond - need to deal with removes from vertex and store in the vertex when edges were first added etc.

class GraphPartition(id:Int) extends Actor {
  val childID = id  //ID which refers to the partitions position in the graph manager map
  var vertices = Map[Int,Vertex]() // Map of Vertices contained in the partition
  var edges = Map[(Int,Int),Edge]() // Map of Edges contained in the partition
  var partitionList = Map[Int,ActorRef]()
  val logging = true

  override def receive: Receive = {

    case PassPartitionList(pl) => partitionList = pl

    case VertexAdd(msgId,srcId) => vertexAdd(msgId,srcId) // If an add vertex command comes in, pass to handler function
    case VertexAddWithProperties(msgId,srcId,properties) => vertexAddWithProperties(msgId,srcId,properties)
    case VertexUpdateProperties(msgId,srcId,properties) => vertexUpdateProperties(msgId,srcId,properties)
    case VertexRemoval(msgId,srcId) => vertexRemoval(msgId,srcId)

    case EdgeAdd(msgId,srcId,destID) => edgeAdd(msgId,srcId,destID)
    case RemoteEdgeAdd(msgId,srcId,dstId) => remoteEdgeAdd(msgId,srcId,dstId)

    case EdgeAddWithProperties(msgId,srcId,dstId,properties) => edgeAddWithProperties(msgId,srcId,dstId,properties)
    case RemoteEdgeAddWithProperties(msgId,srcId,dstId,properties) => remoteEdgeAddWithProperties(msgId,srcId,dstId,properties)

    case EdgeUpdateProperties(msgId,srcId,dstId,properties) => edgeUpdateWithProperties(msgId,srcId,dstId,properties)
    case RemoteEdgeUpdateProperties(msgId,srcId,dstId,properties) => remoteEdgeUpdateWithProperties(msgId,srcId,dstId,properties)

    case EdgeRemoval(msgId,srcId,dstID) => edgeRemoval(msgId,srcId,dstID)
    case RemoteEdgeRemoval(msgId,srcId,dstId) => remoteEdgeRemoval(msgId,srcId,dstId)

  }

  //*******************EDGE BLOCK

  def edgeAdd(msgId:Int,srcId:Int,dstId:Int):Unit={
    if(checkDst(dstId)) localEdge(msgId,true,srcId,dstId) //if dst is also stored in this partition
    else { //if dst is stored in another partition
      remoteEdge(msgId,true,srcId, dstId)
      partitionList(getPartition(dstId)) ! RemoteEdgeAdd(msgId,srcId,dstId)
    }
  }
  def edgeAddWithProperties(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String]):Unit={
    if(checkDst(dstId)) { //check if both vertices are local
      localEdge(msgId,true,srcId,dstId)
      properties.foreach(prop => edges((srcId,dstId)) + (msgId,prop._1,prop._2)) // add all passed properties onto the list
      if(logging) printToFile(s"Edge$srcId-->$dstId",edges(srcId,dstId).printHistory())
    }
    else {
      remoteEdge(msgId,true,srcId,dstId)
      properties.foreach(prop => edges((srcId,dstId)) + (msgId,prop._1,prop._2)) // add all passed properties onto the list
      partitionList(getPartition(dstId)) ! RemoteEdgeAddWithProperties(msgId,srcId,dstId,properties)
      if(logging) printToFile(s"Edge$srcId-->$dstId",edges(srcId,dstId).printHistory())
    }
  }

  def edgeUpdateWithProperties(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String]):Unit={
    if(!edgeExists((srcId,dstId))) edgeAddWithProperties(msgId,srcId,dstId,properties)
    else properties.foreach(prop => edges((srcId,dstId)) + (msgId,prop._1,prop._2)) // add all passed properties onto the list
    edges((srcId,dstId)) match {case re:RemoteEdge => partitionList(re.remotePartitionID) ! RemoteEdgeUpdateProperties(msgId,srcId,dstId,properties) } //inform the other partition

    if(logging) printToFile(s"Edge$srcId-->$dstId",edges(srcId,dstId).printHistory())
  }

  def edgeRemoval(msgId:Int,srcId:Int,dstId:Int):Unit={
    if(!edgeExists((srcId,dstId))){
      if(checkDst(dstId)) localEdge(msgId,false,srcId,dstId) //if dst is also stored in this partition
      else remoteEdge(msgId,false,srcId, dstId) //if dst is stored in another partition
    }
    else edges((srcId,dstId)) kill msgId

    edges((srcId,dstId)) match {case re:RemoteEdge => {partitionList(re.remotePartitionID) ! RemoteEdgeRemoval(msgId,srcId,dstId)}}
    if(logging) printToFile(s"Edge$srcId-->$dstId",edges(srcId,dstId).printHistory())
  }

  //***************** HANDLERS FOR RECEIVING MESSAGE FROM ANOTHER PARTITION
  def remoteEdgeAdd(msgId:Int,srcId:Int,dstId:Int):Unit={
    checkVertexWithEdge(msgId,dstId,srcId,dstId,true) //check if dst exists as a vertex + add associated edges
    if(edges contains (srcId,dstId)) edges((srcId,dstId)) revive msgId //if edge exists add the revive to list
    else edges = edges updated((srcId,dstId),new RemoteEdge(msgId,true,srcId,dstId,RemotePos.Source,getPartition(srcId)))

    if(logging) printToFile(s"RemoteEdge$srcId-->$dstId",edges(srcId,dstId).printHistory())
  }

  def remoteEdgeAddWithProperties(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String]):Unit={
    remoteEdgeAdd(msgId,srcId,dstId)
    properties.foreach(prop => edges((srcId,dstId)) + (msgId,prop._1,prop._2)) // add all passed properties onto the list
    if(logging) printToFile(s"RemoteEdge$srcId-->$dstId",edges(srcId,dstId).printHistory())
  }

  def remoteEdgeUpdateWithProperties(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String]):Unit={
    remoteEdgeAdd(msgId,srcId,dstId)
    properties.foreach(prop => edges((srcId,dstId)) + (msgId,prop._1,prop._2)) // update all passed properties onto the list
    if(logging) printToFile(s"RemoteEdge$srcId-->$dstId",edges(srcId,dstId).printHistory())
  }

  def remoteEdgeRemoval(msgId:Int,srcId:Int,dstId:Int):Unit={
    checkVertexWithEdge(msgId,dstId,srcId,dstId,false) //check if dst exists as a vertex
    if(edges contains (srcId,dstId)) edges((srcId,dstId)) kill msgId //if edge exists add the kill to list
    else edges = edges updated((srcId,dstId),new RemoteEdge(msgId,false,srcId,dstId,RemotePos.Source,getPartition(srcId)))
    if(logging) printToFile(s"RemoteEdge$srcId-->$dstId",edges(srcId,dstId).printHistory())
  }

  //***************** END HANDLERS FOR RECEIVING EDGE ADD FROM ANOTHER PARTITION

  //************ EDGE HELPERS

  def checkDst(dstID:Int):Boolean = if(dstID%partitionList.size==childID) true else false
  def getPartition(ID:Int):Int = ID%partitionList.size
  def edgeExists(srcdst:Tuple2[Int,Int]):Boolean = if(edges contains srcdst) true else false

  def checkVertexWithEdge(msgId:Int,id:Int,srcId:Int,dstId:Int,initialValue:Boolean):Unit= {
    if(!vertices.contains(id)) vertices = vertices updated(id,new Vertex(msgId,id,initialValue))
    vertices(id).addAssociatedEdge(srcId,dstId) //add associated edge to vertices so that if they are removed the edge can also be removed
  }

  def localEdge(msgId:Int,intiailValue:Boolean,srcId:Int,dstId:Int):Unit={
    checkVertexWithEdge(msgId,srcId,srcId,dstId,intiailValue) //check if src and dst both exist as vertices
    checkVertexWithEdge(msgId,dstId,srcId,dstId,intiailValue)
    if(edges contains (srcId,dstId)) edges((srcId,dstId)) revive msgId //if edge exists add the revive to list
    else edges = edges updated((srcId,dstId),new Edge(msgId,intiailValue,srcId,dstId)) // else create new edge

    if(logging) printToFile(s"Edge$srcId-->$dstId",edges(srcId,dstId).printHistory())
  }

  def remoteEdge(msgId:Int,initialValue:Boolean,srcId:Int,dstId:Int):Unit={
    checkVertexWithEdge(msgId,srcId,srcId,dstId,initialValue) //check if src exists as a vertex
    if(edges contains (srcId,dstId)) edges((srcId,dstId)) revive msgId //if edge exists add the revive to list
    else edges = edges updated((srcId,dstId),new RemoteEdge(msgId,initialValue,srcId,dstId,RemotePos.Destination,getPartition(dstId)))

    if(logging) printToFile(s"Edge$srcId-->$dstId",edges(srcId,dstId).printHistory())
  }

  //************ END EDGE HELPERS
  //*******************END EDGE BLOCK

  //*******************VERTEX BLOCK

  def vertexAdd(msgId:Int,srcId:Int): Unit ={ //Vertex add handler function
    if(!(vertices contains srcId))vertices = vertices updated(srcId,new Vertex(msgId,srcId,true)) //if the vertex doesn't already exist, create it and add it to the vertex map
    else vertices(srcId) revive msgId //if it does exist, store the add in the vertex state

    if(logging) printToFile(s"Vertex$srcId",vertices(srcId).printHistory())
  }

  def vertexAddWithProperties(msgId:Int,srcId:Int, properties:Map[String,String]):Unit ={
    vertexAdd(msgId,srcId) //add the vertex
    properties.foreach(l => vertices(srcId) + (msgId,l._1,l._2)) //add all properties
    if(logging) printToFile(s"Vertex$srcId",vertices(srcId).printHistory())
  }

  def vertexUpdateProperties(msgId:Int,srcId:Int,properties:Map[String,String]):Unit = {
    if(!(vertices contains srcId)){ // if the vertex doesn't already exist
      vertices = vertices updated(srcId,new Vertex(msgId,srcId,true)) //create it and add it to the vertex map
    }
    else vertices(srcId) revive msgId //else revive the vertex
    properties.foreach(l => vertices(srcId) + (msgId,l._1,l._2))
    if(logging) printToFile(s"Vertex$srcId",vertices(srcId).printHistory())
  }

  def vertexRemoval(msgId:Int,srcId:Int):Unit={
    if(!(vertices contains srcId)) vertices = vertices updated(srcId,new Vertex(msgId,srcId,false)) //if remove arrives before add create and add remove info
    else vertices(srcId) kill msgId //otherwise if it does already exist kill off
    vertices(srcId).associatedEdges.foreach(pair=> edgeRemoval(msgId,pair._1,pair._2)) //kill all edges
    if(logging) printToFile(s"Vertex$srcId",vertices(srcId).printHistory())
  }

  def vertexExists(srcId:Int)= if(vertices contains srcId) true else false

  //*******************END VERTEX BLOCK

  def printToFile(entityName:String,msg: String):Unit={
    val fw = new FileWriter(s"entityLogs/$entityName.txt")
    try {fw.write(msg+"\n")}
    finally fw.close()
  }


}
