package akka.GraphEntities

/**
  * Class representing Graph Entities
  * Contains a Map of properties (currently String to string)
  * longs representing unique vertex ID's stored in subclassses
  */
class Entity {
  var properties = Map[String,String]()

  def apply(property:String): String = { //overrite the apply method so that we can do vertex("key") to easily retrieve properties
    properties(property)
  }

  def +(key:String,value:String):Unit = { //create + method so can write vertex + (k,v) to easily add new properties
    properties = properties updated (key,value)
    printProperties()
  }

  def printProperties():Unit ={ //test function to make sure the properties are being added to the correct vertices
    println(properties.toString())
  }


}
