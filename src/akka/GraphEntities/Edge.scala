package akka.GraphEntities

/**
  * Created by Mirate on 01/03/2017.
  */
class Edge(msgID:Int,initialValue:Boolean,src: Int, dst:Int) extends Entity(msgID,initialValue){
  val srcID = src
  val dstID = dst

  override def printProperties(): String = s"Edge between $srcID and $dstID: with properties: \n"+ super.printProperties()
}
