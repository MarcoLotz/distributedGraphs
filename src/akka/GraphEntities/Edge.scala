package akka.GraphEntities

/**
  * Created by Mirate on 01/03/2017.
  */
class Edge(src: Int, dst:Int) extends Entity{
  val srcID = src
  val dstID = dst

  override def printProperties(): Unit = {
    println(s"Edge between $srcID and $dstID: with properties:")
    super.printProperties()
  }

}
