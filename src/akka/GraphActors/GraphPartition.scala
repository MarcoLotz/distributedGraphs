package akka.GraphActors

import akka.GraphEntities.{Edge, RemoteEdge, RemotePos, Vertex}
import akka.actor.{Actor, ActorRef}
import java.io._
/**
  * The graph partition manages a set of vertices and there edges
  * Is sent commands which have been processed by the command Processor
  * Will process these, storing information in graph entities which may be updated if they already exist
  * */

//sort random message 1 everywhere
//sort sending remove edge commands to associated edges if they may not exist at that point

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

    case EdgeAdd(msgId,srcId,dstId) => edgeAdd(msgId,srcId,dstId); log(srcId,dstId); log(srcId); log(dstId)
    case RemoteEdgeAdd(msgId,srcId,dstId) => remoteEdgeAdd(msgId,srcId,dstId); rlog(srcId,dstId); log(srcId); log(dstId)

    case EdgeAddWithProperties(msgId,srcId,dstId,properties) => edgeAddWithProperties(msgId,srcId,dstId,properties); log(srcId,dstId); log(srcId); log(dstId)
    case RemoteEdgeAddWithProperties(msgId,srcId,dstId,properties) => remoteEdgeAddWithProperties(msgId,srcId,dstId,properties); rlog(srcId,dstId); log(srcId); log(dstId)

    case EdgeUpdateProperties(msgId,srcId,dstId,properties) => edgeUpdateWithProperties(msgId,srcId,dstId,properties); log(srcId,dstId); log(srcId); log(dstId)
    case RemoteEdgeUpdateProperties(msgId,srcId,dstId,properties) => remoteEdgeUpdateWithProperties(msgId,srcId,dstId,properties); rlog(srcId,dstId); log(srcId); log(dstId)

    case EdgeRemoval(msgId,srcId,dstId) => edgeRemoval(msgId,srcId,dstId); log(srcId,dstId); log(srcId); log(dstId)
    case RemoteEdgeRemoval(msgId,srcId,dstId) => remoteEdgeRemoval(msgId,srcId,dstId); rlog(srcId,dstId); log(srcId); log(dstId)

  }

  def vertexAdd(msgId:Int,srcId:Int): Unit ={ //Vertex add handler function
    if(!(vertices contains srcId))vertices = vertices updated(srcId,new Vertex(msgId,srcId,true)) //if the vertex doesn't already exist, create it and add it to the vertex map
    else vertices(srcId) revive msgId //if it does exist, store the add in the vertex state
  }
  def vertexAddWithProperties(msgId:Int,srcId:Int, properties:Map[String,String]):Unit ={
    vertexAdd(msgId,srcId) //add the vertex
    properties.foreach(l => vertices(srcId) + (msgId,l._1,l._2)) //add all properties
  }



  def edgeAdd(msgId:Int,srcId:Int,dstId:Int):Unit={
    if(checkDst(dstId)) { //local edge
      if(edges contains (srcId,dstId)) edges(srcId,dstId) revive msgId //if the edge already exists revive
      else edges = edges updated((srcId,dstId),new Edge(msgId,true,srcId,dstId)) //if the edge is yet to exist
      vertexAdd(msgId,srcId) //create or revive the source ID
      vertexAdd(msgId,dstId) //do the same for the destination ID
      vertices(srcId) addAssociatedEdge (srcId,dstId) //add the edge to the associated edges of the source node
      vertices(dstId) addAssociatedEdge (srcId,dstId) //do the same for the destination node
    }
    else{ //remote edge
      if(edges contains (srcId,dstId)) edges(srcId,dstId) revive msgId //if the edge already exists revive
      else edges = edges updated((srcId,dstId),new RemoteEdge(msgId,true,srcId,dstId,RemotePos.Destination,getPartition(dstId)))
      vertexAdd(msgId,srcId) //create or revive the source ID
      vertices(srcId) addAssociatedEdge (srcId,dstId) //add the edge to the associated edges of the source node
      partitionList(getPartition(dstId)) ! RemoteEdgeAdd(msgId,srcId,dstId) // inform the partition dealing with the destination node
    }
  }
  def remoteEdgeAdd(msgId:Int,srcId:Int,dstId:Int):Unit={
    if(edges contains (srcId,dstId)) edges((srcId,dstId)) revive msgId //if edge exists add the revive to list
    else edges = edges updated((srcId,dstId),new RemoteEdge(msgId,true,srcId,dstId,RemotePos.Source,getPartition(srcId))) //else create it
    vertexAdd(msgId,dstId) //create or revive the destination node
    vertices(dstId) addAssociatedEdge (srcId,dstId) //add the edge to the associated edges of the destination node
  }



  def edgeAddWithProperties(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String]):Unit={
    if(checkDst(dstId)) { //local edge
      if(edges contains (srcId,dstId)) edges(srcId,dstId) revive msgId //if the edge already exists revive
      else edges = edges updated((srcId,dstId),new Edge(msgId,true,srcId,dstId)) //if the edge is yet to exist
      vertexAdd(msgId,srcId) //create or revive the source ID
      vertexAdd(msgId,dstId) //do the same for the destination ID
      vertices(srcId) addAssociatedEdge (srcId,dstId) //add the edge to the associated edges of the source node
      vertices(dstId) addAssociatedEdge (srcId,dstId) //do the same for the destination node
      properties.foreach(prop => edges((srcId,dstId)) + (msgId,prop._1,prop._2)) // add all passed properties onto the edge
    }
    else{ //remote edge
      if(edges contains (srcId,dstId)) edges(srcId,dstId) revive msgId //if the edge already exists revive
      else edges = edges updated((srcId,dstId),new RemoteEdge(msgId,true,srcId,dstId,RemotePos.Destination,getPartition(dstId)))
      vertexAdd(msgId,srcId) //create or revive the source ID
      vertices(srcId) addAssociatedEdge (srcId,dstId) //add the edge to the associated edges of the source node
      properties.foreach(prop => edges((srcId,dstId)) + (msgId,prop._1,prop._2)) // add all passed properties onto the edge
      partitionList(getPartition(dstId)) ! RemoteEdgeAddWithProperties(msgId,srcId,dstId,properties)
    }
  }
  def remoteEdgeAddWithProperties(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String]):Unit={
    if(edges contains (srcId,dstId)) edges((srcId,dstId)) revive msgId //if edge exists add the revive to list
    else edges = edges updated((srcId,dstId),new RemoteEdge(msgId,true,srcId,dstId,RemotePos.Source,getPartition(srcId))) //else create it
    vertexAdd(msgId,dstId) //create or revive the destination node
    vertices(dstId) addAssociatedEdge (srcId,dstId) //add the edge to the associated edges of the destination node
    properties.foreach(prop => edges((srcId,dstId)) + (msgId,prop._1,prop._2)) // add all passed properties onto the list
  }



  def edgeRemoval(msgId:Int,srcId:Int,dstId:Int):Unit={
    if(checkDst(dstId)) { //local edge
      if(edges contains (srcId,dstId)) edges((srcId,dstId)) kill msgId // if the edge already exists, kill it
      else edges = edges updated((srcId,dstId),new Edge(msgId,false,srcId,dstId)) // otherwise create and initialise as false

      if(!(vertices contains srcId)){ //if src vertex does not exist, create it and wipe the history so that it may contain the associated Edge list
        vertices = vertices updated(srcId,new Vertex(msgId,srcId,true))
        vertices(srcId) wipe()
      }
      if(!(vertices contains dstId)){ //do the same for the destination node
        vertices = vertices updated(dstId,new Vertex(msgId,srcId,true))
        vertices(dstId) wipe()
      }
      vertices(srcId) addAssociatedEdge (srcId,dstId) //add the edge to the associated edges of the source node
      vertices(dstId) addAssociatedEdge (srcId,dstId) //do the same for the destination node
    }
    else {   // remote edge
      if(edges contains (srcId,dstId)) edges((srcId,dstId)) kill msgId // if the edge already exists, kill it
      else edges = edges updated((srcId,dstId),new RemoteEdge(msgId,false,srcId,dstId,RemotePos.Destination,getPartition(dstId))) // otherwise create and initialise as false

      if(!(vertices contains srcId)){ //if src vertex does not exist, create it and wipe the history so that it may contain the associated Edge list
        vertices = vertices updated(srcId,new Vertex(msgId,srcId,true))
        vertices(srcId) wipe()
      }
      vertices(srcId) addAssociatedEdge (srcId,dstId) //add the edge to the associated edges of the source node
      partitionList(getPartition(dstId)) ! RemoteEdgeRemoval(msgId,srcId,dstId) //inform the remote partition of the remove
    }
  }
  def remoteEdgeRemoval(msgId:Int,srcId:Int,dstId:Int):Unit={
    if(edges contains (srcId,dstId)) edges((srcId,dstId)) kill msgId // if the edge already exists, kill it
    else edges = edges updated((srcId,dstId),new Edge(msgId,false,srcId,dstId)) // otherwise create and initialise as false

    if(!(vertices contains dstId)){ //check if the destination node exists, if it does not create it and wipe the history
      vertices = vertices updated(dstId,new Vertex(msgId,srcId,true))
      vertices(dstId) wipe()
    }
    vertices(dstId) addAssociatedEdge (srcId,dstId) //add the edge to the destination nodes associated list

  }







  def vertexRemoval(msgId:Int,srcId:Int):Unit={
    //if(!(vertices contains srcId)) vertices = vertices updated(srcId,new Vertex(msgId,srcId,false)) //if remove arrives before add create and add remove info
    //else vertices(srcId) kill msgId //otherwise if it does already exist kill off
    //vertices(srcId).associatedEdges.foreach(pair=> {
    //  if(edges contains (pair._1,pair._2)) {
    //    edges((pair._1, pair._2)) kill msgId
    //    if(edges((pair._1, pair._2)).isInstanceOf[RemoteEdge]) informRemoteRemove(msgId,pair._1, pair._2)
     // }
    //}) //kill all edges
  }



  //***************** EDGE HELPERS
  def checkDst(dstID:Int):Boolean = if(dstID%partitionList.size==childID) true else false //check if destination is also local
  def getPartition(ID:Int):Int = ID%partitionList.size //get the partition a vertex is stored in
  def addVertex(msgId:Int,id:Int,initialValue:Boolean):Vertex ={ vertices = vertices updated(id,new Vertex(msgId,id,initialValue)); vertices(id)}

  //*******************END EDGE BLOCK

  //*******************PRINT BLOCK
  def printToFile(entityName:String,msg: String):Unit={
    val fw:FileWriter =  if(testPartition) new FileWriter(s"testEntityLogs/$entityName.txt") else new FileWriter(s"entityLogs/$entityName.txt")
    try {fw.write(msg+"\n")}
    finally fw.close()
  }

  def log(srcId:Int):Unit =           if(logging && (vertices contains srcId)) printToFile(s"Vertex$srcId",vertices(srcId).printHistory())
  def log(srcId:Int,dstId:Int):Unit = if(logging && (edges contains (srcId,dstId))) printToFile(s"Edge$srcId-->$dstId",edges(srcId,dstId).printHistory())
  def rlog(srcId:Int,dstId:Int):Unit= if(logging && (edges contains (srcId,dstId))) printToFile(s"RemoteEdge$srcId-->$dstId",edges(srcId,dstId).printHistory())
  //*******************END PRINT BLOCK


  def edgeUpdateWithProperties(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String]):Unit= edgeAddWithProperties(msgId,srcId,dstId,properties)
  def vertexUpdateProperties(msgId:Int,srcId:Int,properties:Map[String,String]):Unit = vertexAddWithProperties(msgId,srcId,properties)
  def remoteEdgeUpdateWithProperties(msgId:Int,srcId:Int,dstId:Int,properties:Map[String,String]):Unit= remoteEdgeAddWithProperties(msgId,srcId,dstId,properties)
}
