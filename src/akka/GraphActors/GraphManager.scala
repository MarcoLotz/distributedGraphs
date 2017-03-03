package akka.GraphActors

import akka.actor.{Actor, ActorRef, Props}
import com.oracle.webservices.internal.api.message.PropertySet.Property


/**
  * The Graph Manager is the top level actor in this system (under the stream)
  * which tracks all the graph partitions - passing commands processed by the 'command processor' actors
  * to the correct partition
  */


//The following block are all case classes (commands) which the manager can handle

case class InitilizeGraph(children:Int) //class for starting the manager, passes the initial number of partitions

case class VertexAdd(srcId:Int) //add a vertex (or add/update a property to an existing vertex)
case class VertexAddWithProperties(srcId:Int, properties: Map[String,String])
case class VertexUpdateProperties(srcId:Int, propery:Tuple2[String,String])
case class VertexRemove()

case class EdgeAdd(srcId:Int,destID:Int,property:Tuple2[String,String])
case class EdgeRemove()
case class EdgeUpdate()

class GraphManager extends Actor{
  var running = false // bool to check if graph has already been initialized
  var childMap = Map[Int,ActorRef]() // map of graph partitions
  var children = 0 // var to store number of children

  override def receive: Receive = {
    case InitilizeGraph(children) => initilizeGraph(children)
    case VertexAdd(srcId) => childMap(chooseChild(srcId)) ! VertexAdd(srcId) //select handling partition and forward VertexAdd command
    case VertexAddWithProperties(srcId,properties) => childMap(chooseChild(srcId)) ! VertexAddWithProperties(srcId,properties)
    case VertexUpdateProperties(srcId,propery) => childMap(chooseChild(srcId)) ! VertexUpdateProperties(srcId,propery)
    case _ => println("message not recognized!")
  }


  def initilizeGraph(children:Int):Unit = {
    if (running) println("Warning: duplicate start message received") // do not reinitialise the graph

    else { // during initialisation
      running = true //set running flag to true
      this.children = children //set the number of children to passed value
      for(i <- 0 until children){
        val child =  context.actorOf(Props(new GraphPartition(i))) //create graph partitions
        childMap = childMap updated (i,child) //and add to partition map
      }
    }
  }

  def chooseChild(srcId:Int):Int = { //simple srcID hash at the moment
    srcId % children
  }

}
