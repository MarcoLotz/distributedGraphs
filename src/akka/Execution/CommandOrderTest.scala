package akka.Execution

import java.io.File

import scala.io.Source

/**
  * Created by Mirate on 06/04/2017.
  */
object CommandOrderTest extends App{


  val serialised = entityFileMap("testEntityLogs")
  val toTest = entityFileMap("EntityLogs")
  //toTest.foreach(f => println(f))
  compareTo()

  def compareTo():Unit={

    val remoteCount = toTest.size - serialised.size
    var count = 0
    serialised.foreach(f => {
     println(s"Testing: ${f._1}")
     if(toTest contains f._1) file2file(file2lines(f._2),file2lines(toTest(f._1)))
     else println(s"File ${f._1} does not exist in entityLogs")

     if((f._1 contains "Edge") && (toTest contains s"Remote${f._1}")) {
       println("Testing remote edge:")
       file2file(file2lines(f._2),file2lines(toTest(s"Remote${f._1}")))
       count+=1
     }
    })
    if(count!=remoteCount)println("Remote count off!") else println("correct remote count")
  }

  def file2file(sFile:List[String],tFile:List[String]):Unit={
    if(sFile==Nil){
      if(tFile==Nil) println("    Both files ended at the same time")
      else println("    sFile ended, but tFile still has more")
    }
    else if(tFile==Nil){
      if(sFile==Nil) println("     Both files ended at the same time")
      else println("    tFile ended, but sFile still has more")
    }
    else if(sFile.head==tFile.head){
      file2file(sFile.tail,tFile.tail)
    }
    else {
      println(s"    Line mismatch! sfile has ${sFile.head} whilst tfile has ${tFile.head}")
    }

  }

  def file2lines(file:File):List[String] = {
    try {Source.fromFile(file).getLines().toList}
    catch {case e: Exception => println(s"ERROR! ${file.getName}"); List()}
  }

  def entityFileMap(dir: String):Map[String,File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      var map = Map[String,File]()
      d.listFiles.filter(_.isFile).foreach(f => map = map updated (f.getName,f))
      map.filter(p => p._1.charAt(0)!='.') // filter out any hidden files at github love to put them everywhere
    } else {
      Map[String,File]()
    }
  }
}
