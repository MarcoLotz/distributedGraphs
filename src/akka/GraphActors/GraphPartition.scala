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

//reminder to self, need to think of way of dealing with requests for remote vertex info
//also should probably store information within the edge to say where it is also being stored

class GraphPartition(id:Int) extends Actor {
  val childID = id  //ID which refers to the partitions position in the graph manager map
  var vertices = Map[Int,Vertex]() // Map of Vertices contained in the partition
  var edges = Map[(Int,Int),Edge]() // Map of Edges contained in the partition
  var partitionList = Map[Int,ActorRef]()

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
    if(checkDst(dstId)) localEdge(msgId,srcId,dstId) //if dst is also stored in this partition
    else { //if dst is stored in another partition
      remoteEdge(msgId,srcId, dstId)
      partitionList(getDstPartition(dstId)) ! RemoteEdgeAdd(msgId,srcId,dstId)
    }
  }
  def edgeAddWithProperties(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String]):Unit={
    if(checkDst(dstId)) { //check if both vertices are local
      localEdge(msgId,srcId,dstId)
      properties.foreach(prop => edges((srcId,dstId)) + (msgId,prop._1,prop._2)) // add all passed properties onto the list
      printToFile(s"Edge$srcId-->$dstId",edges(srcId,dstId).printHistory())
    }
    else {
      remoteEdge(msgId,srcId,dstId)
      properties.foreach(prop => edges((srcId,dstId)) + (msgId,prop._1,prop._2)) // add all passed properties onto the list
      partitionList(getDstPartition(dstId)) ! RemoteEdgeAddWithProperties(msgId,srcId,dstId,properties)
      printToFile(s"Edge$srcId-->$dstId",edges(srcId,dstId).printHistory())
    }
  }

  def edgeUpdateWithProperties(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String]):Unit={
    if(!edgeExists((srcId,dstId))){
      edgeAddWithProperties(msgId,srcId,dstId,properties)
      printToFile(s"Edge$srcId-->$dstId",edges(srcId,dstId).printHistory())
    }
    edges((srcId,dstId)) match {
      case re:RemoteEdge => partitionList(re.remotePartitionID) ! RemoteEdgeUpdateProperties(msgId,srcId,dstId,properties)
      case e:Edge => {
        properties.foreach(prop => edges((srcId,dstId)) + (msgId,prop._1,prop._2)) // add all passed properties onto the list
        printToFile(s"Edge$srcId-->$dstId",edges(srcId,dstId).printHistory())
      }
    }
  }

  def edgeRemoval(msgId:Int,srcId:Int,dstId:Int):Unit={
    if(edgeExists((srcId,dstId))){
      edges((srcId,dstId)) match {
        case re:RemoteEdge => {
          edges = edges - ((srcId,dstId))
          partitionList(re.remotePartitionID) ! RemoteEdgeRemoval(msgId,srcId,dstId)
        }
        case e:Edge => edges = edges - ((srcId,dstId))
      }
    }
    printToFile(s"Edge$srcId-->$dstId",edges(srcId,dstId).printHistory())
  }

  //***************** HANDLERS FOR RECEIVING MESSAGE FROM ANOTHER PARTITION
  def remoteEdgeAdd(msgId:Int,srcId:Int,dstId:Int):Unit={
    if(!vertices.contains(dstId)) vertexAdd(msgId,dstId) //check if dst exists as a vertex
    vertices(dstId).addAssociatedEdge(srcId,dstId)
    edges = edges updated((srcId,dstId),new RemoteEdge(srcId,dstId,RemotePos.Source,getDstPartition(srcId)))
    //create 'remote edge' tracking the original source of the command where the src node is stored
    printToFile(s"Edge$srcId-->$dstId",edges(srcId,dstId).printHistory())
  }

  def remoteEdgeAddWithProperties(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String]):Unit={
    if(!vertices.contains(dstId)) vertexAdd(msgId,dstId) //check if dst exists as a vertex
    vertices(dstId).addAssociatedEdge(srcId,dstId)
    edges = edges updated((srcId,dstId),new RemoteEdge(srcId,dstId,RemotePos.Source,getDstPartition(srcId)))
    properties.foreach(prop => edges((srcId,dstId)) + (msgId,prop._1,prop._2)) // add all passed properties onto the list
    printToFile(s"Edge$srcId-->$dstId",edges(srcId,dstId).printHistory())
  }

  def remoteEdgeUpdateWithProperties(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String]):Unit={
    properties.foreach(prop => edges((srcId,dstId)) + (msgId,prop._1,prop._2)) // update all passed properties onto the list
    printToFile(s"Edge$srcId-->$dstId",edges(srcId,dstId).printHistory())
  }

  def remoteEdgeRemoval(msgId:Int,srcId:Int,dstId:Int):Unit={
    edges = edges - ((srcId,dstId))
    printToFile(s"Edge$srcId-->$dstId",edges(srcId,dstId).printHistory())
  }

  //***************** END HANDLERS FOR RECEIVING EDGE ADD FROM ANOTHER PARTITION

  //************ EDGE HELPERS

  def checkDst(dstID:Int):Boolean = if(dstID%partitionList.size==childID) true else false
  def getDstPartition(dstID:Int):Int = dstID%partitionList.size
  def edgeExists(srcdst:Tuple2[Int,Int]):Boolean = if(edges contains srcdst) true else false

  def localEdge(msgId:Int,srcId:Int,dstId:Int):Unit={
    if(!vertices.contains(srcId)) vertexAdd(msgId,srcId) //check if src and dst both exist as vertices
    if(!vertices.contains(dstId)) vertexAdd(msgId,dstId)
    vertices(srcId).addAssociatedEdge(srcId,dstId) //add associated edge to vertices so that if they are removed
    vertices(dstId).addAssociatedEdge(srcId,dstId) //the edge can also be removed
    edges = edges updated((srcId,dstId),new Edge(srcId,dstId)) // add local edge
    printToFile(s"Edge$srcId-->$dstId",edges(srcId,dstId).printHistory())
  }
  def remoteEdge(msgId:Int,srcId:Int,dstId:Int):Unit={
    if(!vertices.contains(srcId)) vertexAdd(msgId,srcId) //check if src exists as a vertex
    vertices(srcId).addAssociatedEdge(srcId,dstId)
    edges = edges updated((srcId,dstId),new RemoteEdge(srcId,dstId,RemotePos.Destination,getDstPartition(dstId)))
    //add 'remote' edge tracking the location of the destination vetex
    printToFile(s"Edge$srcId-->$dstId",edges(srcId,dstId).printHistory())
  }

  //************ END EDGE HELPERS
  //*******************END EDGE BLOCK

  //*******************VERTEX BLOCK

  def vertexAdd(msgId:Int,srcId:Int): Unit ={ //Vertex add handler function
    if(!(vertices contains srcId)){ // if the vertex doesn't already exist
      vertices = vertices updated(srcId,new Vertex(msgId,srcId)) //create it and add it to the vertex map
    }
    else vertices(srcId) revive msgId //if it does exist, store the add in the vertex state

    printToFile(s"Vertex$srcId",vertices(srcId).printHistory())
  }

  def vertexAddWithProperties(msgId:Int,srcId:Int, properties:Map[String,String]):Unit ={
    vertexAdd(msgId,srcId) //add the vertex
    vertexUpdateProperties(msgId,srcId,properties) //add all properties
  }

  def vertexUpdateProperty(msgId:Int,srcId:Int, property:(String,String)):Unit ={
    if(!(vertices contains srcId)){ // if the vertex doesn't already exist
      vertices = vertices updated(srcId,new Vertex(msgId,srcId)) //create it and add it to the vertex map
    }
    vertices(srcId) + (msgId,property._1,property._2) //add the new property
  }
  def vertexUpdateProperties(msgId:Int,srcId:Int,properties:Map[String,String]):Unit = {
    properties.foreach(l => vertexUpdateProperty(msgId,srcId,(l._1,l._2)))
    printToFile(s"Vertex$srcId",vertices(srcId).printHistory())
  }

  def vertexRemoval(msgId:Int,srcId:Int):Unit={
      if(vertices contains srcId){
        vertices(srcId).associatedEdges.foreach(pair=> edgeRemoval(msgId,pair._1,pair._2)) //removal all associated edges with the vertex (this will also handle other partitions
        vertices = vertices - srcId
        printToFile(s"Vertex$srcId",vertices(srcId).printHistory())
      }
  }

  def vertexExists(srcId:Int)= if(vertices contains srcId) true else false

  //*******************END VERTEX BLOCK

  def printToFile(entityName:String,msg: String):Unit={
    val fw = new FileWriter(s"entityLogs/$entityName.txt")
    try {fw.write(msg+"\n")}
    finally fw.close()
  }


}
