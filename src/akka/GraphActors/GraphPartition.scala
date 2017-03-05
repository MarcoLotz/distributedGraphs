package akka.GraphActors

import akka.GraphEntities.{Edge, Vertex}
import akka.actor.{Actor, ActorRef}

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

    case VertexAdd(srcId) => vertexAdd(srcId) // If an add vertex command comes in, pass to handler function
    case VertexAddWithProperties(srcId,properties) => vertexAddWithProperties(srcId,properties)
    case VertexUpdateProperties(srcId,properties) => vertexUpdateProperties(srcId,properties)

    case EdgeAdd(srcId,destID) => edgeAdd(srcId,destID)
    case RemoteEdgeAdd(srcId,dstId) => remoteEdgeAdd(srcId,dstId)

    case EdgeAddWithProperties(srcId,dstId,properties) => edgeAddWithProperties(srcId,dstId,properties)
    case RemoteEdgeAddWithProperties(srcId,dstId,properties) => remoteEdgeAddWithProperties(srcId,dstId,properties)

  }

  //*******************EDGE BLOCK

  def edgeAdd(srcId:Int,dstId:Int):Unit={
    if(checkDst(dstId)) localEdge(srcId,dstId) //if dst is also stored in this partition
    else { //if dst is sotred in another partition
      remoteEdge(srcId, dstId)
      partitionList(getDstPartition(dstId)) ! RemoteEdgeAdd(srcId,dstId)
    }
  }

  def edgeAddWithProperties(srcId:Int,dstId:Int,properties:Map[String,String]):Unit={
    if(checkDst(dstId)) { //check if both vertices are local
      localEdge(srcId,dstId)
      properties.foreach(prop => edges((srcId,dstId)) + (prop._1,prop._2)) // add all passed properties onto the list
    }
    else {
      remoteEdge(srcId,dstId)
      properties.foreach(prop => edges((srcId,dstId)) + (prop._1,prop._2)) // add all passed properties onto the list
      partitionList(getDstPartition(dstId)) ! RemoteEdgeAddWithProperties(srcId,dstId,properties)
    }
  }

  def remoteEdgeAdd(srcId:Int,dstId:Int):Unit={
    println(s"received shared edge in $childID")
    if(!(vertices.contains(dstId))) vertexAdd(dstId) //check if dst exists as a vertex
    edges = edges updated((srcId,dstId),new Edge(srcId,dstId)) // add local edge
  }

  def remoteEdgeAddWithProperties(srcId:Int,dstId:Int,properties:Map[String,String]):Unit={
    println(s"received shared edge in $childID")
    if(!(vertices.contains(dstId))) vertexAdd(dstId) //check if dst exists as a vertex
    edges = edges updated((srcId,dstId),new Edge(srcId,dstId)) // add local edge
    properties.foreach(prop => edges((srcId,dstId)) + (prop._1,prop._2)) // add all passed properties onto the list
  }


  //************ EDGE HELPERS

  def checkDst(dstID:Int):Boolean = if(dstID%partitionList.size==childID) true else false
  def getDstPartition(dstID:Int):Int = dstID%partitionList.size

  def localEdge(srcId:Int,dstId:Int):Unit={
    println(s"Fully local edge in $childID")
    if(!(vertices.contains(srcId))) vertexAdd(srcId) //check if src and dst both exist as vertices
    if(!(vertices.contains(dstId))) vertexAdd(dstId)
    edges = edges updated((srcId,dstId),new Edge(srcId,dstId)) // add local edge
  }

  def remoteEdge(srcId:Int,dstId:Int):Unit={
    println(s"Shared edge in $childID")
    if(!(vertices.contains(srcId))) vertexAdd(srcId) //check if src exists as a vertex
    edges = edges updated((srcId,dstId),new Edge(srcId,dstId)) // add local edge

  }

  //************ END EDGE HELPERS




  //*******************END EDGE BLOCK

  //*******************VERTEX BLOCK

  def vertexAdd(srcId:Int): Unit ={ //Vertex add handler function
    println(s"$childID dealing with $srcId ") // println checking which partition is dealing with that vertex
    if(!(vertices contains srcId)){ // if the vertex doesn't already exist
      vertices = vertices updated(srcId,new Vertex(srcId)) //create it and add it to the vertex map
    }
  }

  def vertexUpdateProperty(srcId:Int, property:Tuple2[String,String]):Unit ={
    if(!(vertices contains srcId)){ // if the vertex doesn't already exist
      vertices = vertices updated(srcId,new Vertex(srcId)) //create it and add it to the vertex map
    }
    vertices(srcId) + (property._1,property._2) //add the new property
  }

  def vertexAddWithProperties(srcId:Int, properties:Map[String,String]):Unit ={
      vertexAdd(srcId)
      vertexUpdateProperties(srcId,properties)
  }

  def vertexUpdateProperties(srcId:Int,properties:Map[String,String]):Unit = properties.foreach(l => vertexUpdateProperty(srcId,(l._1,l._2)))


  //*******************END VERTEX BLOCK

}
