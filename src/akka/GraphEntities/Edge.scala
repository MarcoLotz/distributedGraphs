package akka.GraphEntities

/**
  * Created by Mirate on 01/03/2017.
  */
class Edge(src: Int, dst:Int) extends Entity(1,true){
  val srcID = src
  val dstID = dst

  override def printProperties(): String = s"Edge between $srcID and $dstID: with properties: \n"+ super.printProperties()
}
