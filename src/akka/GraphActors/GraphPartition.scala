package akka.GraphActors

import akka.GraphEntities.Vertex
import akka.actor.Actor

/**
  * The graph partition manages a set of vertices and there edges
  * Is sent commands which have been processed by the command Processor
  * Will process these, storing information in graph entities which may be updated if they already exist
  *
  * */

class GraphPartition(id:Int) extends Actor {
  val childID = id  //ID which refers to the partitions position in the graph manager map
  var vertices = Map[Int,Vertex]() // Map of Vertices contained in the partition

  override def receive: Receive = {
    case VertexAdd(srcId) => vertexAdd(srcId) // If an add vertex command comes in, pass to handler function
    case VertexAddWithProperties(srcId,properties) => vertexAddWithProperties(srcId,properties)
    case VertexUpdateProperties(srcId,propery) => vertexUpdateProperties(srcId,propery)
  }

  def vertexAdd(srcId:Int): Unit ={ //Vertex add handler function
    println(s"$childID dealing with $srcId ") // println checking which partition is dealing with that vertex
    if(!(vertices contains srcId)){ // if the vertex doesn't already exist
      vertices = vertices updated(srcId,new Vertex(srcId)) //create it and add it to the vertex map
    }
  }

  def vertexAddWithProperties(srcId:Int, properties:Map[String,String]):Unit ={
      vertexAdd(srcId)
      properties.foreach(l => vertexUpdateProperties(srcId,(l._1,l._2)))
  }

  def vertexUpdateProperties(srcId:Int, property:Tuple2[String,String]):Unit ={
    if(!(vertices contains srcId)){ // if the vertex doesn't already exist
      vertices = vertices updated(srcId,new Vertex(srcId)) //create it and add it to the vertex map
    }
    vertices(srcId) + (property._1,property._2) //add the new property
  }


}
