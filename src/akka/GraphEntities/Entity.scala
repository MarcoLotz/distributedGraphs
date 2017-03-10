package akka.GraphEntities

/**
  * Class representing Graph Entities
  * Contains a Map of properties (currently String to string)
  * longs representing unique vertex ID's stored in subclassses
  */

//corner case of if remove turns up before entity created, should this remove create the entity and then instantly make it deleted or should the command be dropped

//initially created
//currently alive
//previous states / are there previous states

class Entity(creationMessage:Int) {
  var properties = Map[String,Property]()
  val initiallyCreated = creationMessage
  var previousState:List[(Int,Boolean)] = (creationMessage,true)::Nil

  def currentlyAlive():Boolean = previousState.head._2 //check front pos of list

  def revive(msgID:Int):Unit={
    if(msgID > previousState.head._1) previousState = (msgID,true) :: previousState //if the revive can go at the front of the list, then just add it
    else previousState = previousState.head :: reviveHelper(msgID,previousState.tail) //otherwise we need to find where it goes by looking through the list
  }
  private def reviveHelper(msgID:Int,ps:List[(Int,Boolean)]):List[(Int,Boolean)] ={
    if(ps isEmpty) (msgID,true)::Nil //somehow reached the end of the list
    else if(msgID > ps.head._1) (msgID,true) :: ps //if we have found the position the command should go in the list, return it at the head of the ps
    else ps.head :: reviveHelper(msgID,ps.tail) //otherwise keep looking
  }

  def kill(msgID:Int):Unit={
    if(msgID > previousState.head._1) previousState = (msgID,false) :: previousState //if the kill is the latest command put at the front
    else previousState = previousState.head :: conspireToCommitMurder(msgID,previousState.tail) //otherwise we need to find where it goes by looking through the list
    properties.foreach(p => p._2.kill(msgID)) //send the message to all properties
  }

  private def conspireToCommitMurder(msgID:Int,ps:List[(Int,Boolean)]):List[(Int,Boolean)] ={
    if(ps isEmpty) (msgID,false)::Nil //somehow reached the end of the list
    else if(msgID > ps.head._1) (msgID,false) :: ps //if we have found the position the command should go in the list, return it at the head of the ps
    else ps.head :: reviveHelper(msgID,ps.tail) //otherwise keep looking
  }


  def apply(property:String): Property = { //overrite the apply method so that we can do vertex("key") to easily retrieve properties
    properties(property)
  }

  def +(msgID:Int,key:String,value:String):Unit = { //create + method so can write vertex + (k,v) to easily add new properties
    if(properties contains key) properties(key) update (msgID,value)
    else properties = properties updated (key,new Property(msgID,key,value))
  }

  def printHistory():String={
    var toReturn="Previous state of entity: \n"
    previousState.foreach(p => toReturn = s"$toReturn MessageID ${p._1}: ${p._2} \n")

    s"$toReturn \n $printProperties" //print previous state of entity + properties -- title left off as will be done in subclass
  }

  def printProperties():String ={ //test function to make sure the properties are being added to the correct vertices
    var toReturn ="" //indent to be inside the entity
    properties.foreach(p => toReturn = s"$toReturn      ${p._2.toString} \n")
    toReturn
  }


}
