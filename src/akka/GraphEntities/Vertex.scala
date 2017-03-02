package akka.GraphEntities

/**
  * Class representing Grpah Vertices
  */

class Vertex(id:Int) extends Entity{
  val vertexId:Long = id

  override def printProperties(): Unit = {
    println(s"Vertex $vertexId with properties:")
    super.printProperties()
  }

}
