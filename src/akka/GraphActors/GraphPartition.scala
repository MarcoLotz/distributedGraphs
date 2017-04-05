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

class GraphPartition(id:Int,test:Boolean) extends Actor {
  val childID = id  //ID which refers to the partitions position in the graph manager map
  var vertices = Map[Int,Vertex]() // Map of Vertices contained in the partition
  var edges = Map[(Int,Int),Edge]() // Map of Edges contained in the partition
  var partitionList = Map[Int,ActorRef]()
  val logging = true
  val testPartition = test

  override def receive: Receive = {

    case PassPartitionList(pl) => partitionList = pl

    case VertexAdd(msgId,srcId) => vertexAdd(msgId,srcId); log(srcId)
    case VertexAddWithProperties(msgId,srcId,properties) => vertexAddWithProperties(msgId,srcId,properties); log(srcId)
    case VertexUpdateProperties(msgId,srcId,properties) => vertexUpdateProperties(msgId,srcId,properties); log(srcId)
    case VertexRemoval(msgId,srcId) => vertexRemoval(msgId,srcId); log(srcId)

    case EdgeAdd(msgId,srcId,dstId) => edgeAdd(msgId,srcId,dstId); log(srcId,dstId)
    case RemoteEdgeAdd(msgId,srcId,dstId) => remoteEdgeAdd(msgId,srcId,dstId); rlog(srcId,dstId)

    case EdgeAddWithProperties(msgId,srcId,dstId,properties) => edgeAddWithProperties(msgId,srcId,dstId,properties); log(srcId,dstId)
    case RemoteEdgeAddWithProperties(msgId,srcId,dstId,properties) => remoteEdgeAddWithProperties(msgId,srcId,dstId,properties); rlog(srcId,dstId)

    case EdgeUpdateProperties(msgId,srcId,dstId,properties) => edgeUpdateWithProperties(msgId,srcId,dstId,properties); log(srcId,dstId)
    case RemoteEdgeUpdateProperties(msgId,srcId,dstId,properties) => remoteEdgeUpdateWithProperties(msgId,srcId,dstId,properties); rlog(srcId,dstId)

    case EdgeRemoval(msgId,srcId,dstId) => edgeRemoval(msgId,srcId,dstId); log(srcId,dstId)
    case RemoteEdgeRemoval(msgId,srcId,dstId) => remoteEdgeRemoval(msgId,srcId,dstId); rlog(srcId,dstId)

  }
  //*******************VERTEX BLOCK
  def vertexAdd(msgId:Int,srcId:Int): Unit ={ //Vertex add handler function
    if(!(vertices contains srcId))vertices = vertices updated(srcId,new Vertex(msgId,srcId,true)) //if the vertex doesn't already exist, create it and add it to the vertex map
    else vertices(srcId) revive msgId //if it does exist, store the add in the vertex state
  }
  def vertexAddWithProperties(msgId:Int,srcId:Int, properties:Map[String,String]):Unit ={
    vertexAdd(msgId,srcId) //add the vertex
    properties.foreach(l => vertices(srcId) + (msgId,l._1,l._2)) //add all properties
  }
  def vertexRemoval(msgId:Int,srcId:Int):Unit={
    if(!(vertices contains srcId)) vertices = vertices updated(srcId,new Vertex(msgId,srcId,false)) //if remove arrives before add create and add remove info
    else vertices(srcId) kill msgId //otherwise if it does already exist kill off
    vertices(srcId).associatedEdges.foreach(pair=> edgeRemoval(msgId,pair._1,pair._2)) //kill all edges
  }

  //*******************EDGE BLOCK

  def edgeAdd(msgId:Int,srcId:Int,dstId:Int):Unit={
    if(checkDst(dstId)) localEdge(msgId,srcId,dstId,true) //if dst is also stored in this partition
    else {remoteEdge(msgId,srcId,dstId,true); informRemoteAdd(msgId,srcId,dstId)}  //if dst is stored in another partition
  }
  def edgeAddWithProperties(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String]):Unit={
    if(checkDst(dstId)) localEdge(msgId,srcId,dstId,true) //check if both vertices are local
    else {remoteEdge(msgId,srcId,dstId,true); informRemoteAddProp(msgId,srcId,dstId,properties)}
    properties.foreach(prop => edges((srcId,dstId)) + (msgId,prop._1,prop._2)) // add all passed properties onto the list
  }
  def edgeRemoval(msgId:Int,srcId:Int,dstId:Int):Unit={
    if(checkDst(dstId)) localEdge(msgId,srcId,dstId,true)
    else {remoteEdge(msgId,srcId,dstId,true); informRemoteRemove(msgId,srcId,dstId)}
  }

  def informRemoteAdd(msgId:Int,srcId:Int,dstId:Int):Unit =  partitionList(getPartition(dstId)) ! RemoteEdgeAdd(msgId,srcId,dstId)
  def informRemoteAddProp(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String]):Unit= partitionList(getPartition(dstId)) ! RemoteEdgeAddWithProperties(msgId,srcId,dstId,properties)
  def informRemoteRemove(msgId:Int,srcId:Int,dstId:Int):Unit = partitionList(edges((srcId,dstId)).asInstanceOf[RemoteEdge].remotePartitionID) ! RemoteEdgeRemoval(msgId,srcId,dstId)

  //***************** REMOTE EDGE BLOCK
  def remoteEdgeAdd(msgId:Int,srcId:Int,dstId:Int):Unit={
    checkVertexWithEdge(msgId,dstId,srcId,dstId,true) //check if dst exists as a vertex + add associated edges
    if(edges contains (srcId,dstId)) edges((srcId,dstId)) revive msgId //if edge exists add the revive to list
    else addRemoteEdgeS(msgId,srcId,dstId,true) //create the remote edge
  }
  def remoteEdgeAddWithProperties(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String]):Unit={
    remoteEdgeAdd(msgId,srcId,dstId)
    properties.foreach(prop => edges((srcId,dstId)) + (msgId,prop._1,prop._2)) // add all passed properties onto the list
  }
  def remoteEdgeRemoval(msgId:Int,srcId:Int,dstId:Int):Unit={
    checkVertexWithEdge(msgId,dstId,srcId,dstId,false) //check if dst exists as a vertex
    if(edges contains (srcId,dstId)) edges((srcId,dstId)) kill msgId //if edge exists add the kill to list
    else addRemoteEdgeD(msgId,srcId,dstId,false) //else create but initialise as false
  }

  //***************** EDGE HELPERS
  def checkDst(dstID:Int):Boolean = if(dstID%partitionList.size==childID) true else false //check if destination is also local
  def getPartition(ID:Int):Int = ID%partitionList.size //get the partition a vertex is stored in
  def addRemoteEdgeS(msgId:Int,srcId:Int,dstId:Int,initialValue:Boolean):Unit = edges = edges updated((srcId,dstId),new RemoteEdge(msgId,initialValue,srcId,dstId,RemotePos.Source,getPartition(srcId)))
  def addRemoteEdgeD(msgId:Int,srcId:Int,dstId:Int,initialValue:Boolean):Unit = edges = edges updated((srcId,dstId),new RemoteEdge(msgId,initialValue,srcId,dstId,RemotePos.Destination,getPartition(dstId)))
  def addEdge(msgId:Int,srcId:Int,dstId:Int,initialValue:Boolean):Unit = edges = edges updated((srcId,dstId),new Edge(msgId,initialValue,srcId,dstId))
  def addVertex(msgId:Int,id:Int,initialValue:Boolean):Vertex ={ vertices = vertices updated(id,new Vertex(msgId,id,initialValue)); vertices(id)}

  def localEdge(msgId:Int, srcId:Int, dstId:Int, initialValue:Boolean):Unit={
    checkVertexWithEdge(msgId,srcId,srcId,dstId,initialValue); checkVertexWithEdge(msgId,dstId,srcId,dstId,initialValue) //check if src and dst both exist as vertices
    if((edges contains (srcId,dstId)) && initialValue) edges((srcId,dstId)) revive msgId //if edge exists and addEdge called the function - add the revive to list
    else if((edges contains (srcId,dstId)) && !initialValue) edges((srcId,dstId)) kill msgId //if edge exists and removeEdge called the function - add the kill to list
    else addEdge(msgId,srcId,dstId,initialValue) // else create new edge
  }
  def remoteEdge(msgId:Int, srcId:Int, dstId:Int, initialValue:Boolean):Unit={
    checkVertexWithEdge(msgId,srcId,srcId,dstId,initialValue) //check if src exists as a vertex
    if((edges contains (srcId,dstId)) && initialValue) edges((srcId,dstId)) revive msgId //if edge exists and addEdge called the function - add the revive to list
    else if((edges contains (srcId,dstId)) && !initialValue) edges((srcId,dstId)) kill msgId //if edge exists and removeEdge called the function - add the kill to list
    else addRemoteEdgeD(msgId,srcId,dstId,initialValue) //else create the remote edge
  }

  def checkVertexWithEdge(msgId:Int,id:Int,srcId:Int,dstId:Int,initialValue:Boolean):Unit= {
    if(initialValue) vertexAdd(msgId,id)
    else if(!vertices.contains(id)) addVertex(msgId,id,initialValue).wipe() //if the command is a remove and the vertex does not exist, first create it then wipe it as the vertex was not 'killed' at this point, if it was run sequentially the kill would not be in the history
    vertices(id).addAssociatedEdge(srcId,dstId) //add associated edge to vertices so that if they are removed the edge can also be removed
  }
  //*******************END EDGE BLOCK

  //*******************PRINT BLOCK
  def printToFile(entityName:String,msg: String):Unit={
    val fw = if(testPartition) new FileWriter(s"testEntityLogs/$entityName.txt") else new FileWriter(s"entityLogs/$entityName.txt")
    try {fw.write(msg+"\n")}
    finally fw.close()
  }

  def log(srcId:Int):Unit =           if(logging) printToFile(s"Vertex$srcId",vertices(srcId).printHistory())
  def log(srcId:Int,dstId:Int):Unit = if(logging) printToFile(s"Edge$srcId-->$dstId",edges(srcId,dstId).printHistory())
  def rlog(srcId:Int,dstId:Int):Unit= if(logging) printToFile(s"RemoteEdge$srcId-->$dstId",edges(srcId,dstId).printHistory())
  //*******************END PRINT BLOCK


  def edgeUpdateWithProperties(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String]):Unit= edgeAddWithProperties(msgId,srcId,dstId,properties)
  def vertexUpdateProperties(msgId:Int,srcId:Int,properties:Map[String,String]):Unit = vertexAddWithProperties(msgId,srcId,properties)
  def remoteEdgeUpdateWithProperties(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String]):Unit= remoteEdgeAddWithProperties(msgId,srcId,dstId,properties)
}
