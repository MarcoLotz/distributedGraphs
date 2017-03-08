package akka.Execution

import java.time.Instant

import sys.process._
import akka.GraphActors._
import akka.GraphActors.GraphManager
import akka.actor.{ActorRef, ActorSystem, Props}
import akka.kafka.scaladsl.Consumer
import akka.kafka.{ConsumerSettings, Subscriptions}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.Sink
import com.typesafe.config.ConfigFactory
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer}

import scala.concurrent.Await
import scala.concurrent.duration._

object ConsumerTest extends App {

  val config = ConfigFactory.load() // load config (TODO: mess with config)
  implicit val system = ActorSystem.create("akka-stream", config) //create the actor system
  implicit val mat = ActorMaterializer() //Dunno what this does lol, but need it for the stream

  var commandCount = 0;
  val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer) //pass actor system and deserializer for message K/V
    .withBootstrapServers("localhost:9092") //specify where the bootstrap server for kafka is
    .withGroupId("akka-stream-kafka-test") //group id for the consumer, note that offsets are always committed for a given consumer group
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") // tuning params

  var childMap = Map[Int,ActorRef]() // map of graph partitions
  var managerMap = Map[Int,ActorRef]() //create map of command processors
  val managerCount = 4 //Number of Command Processors (Incoming String to Case class)
  val childCount = 3 // number of graph partitions
  initilizeGraph() //fill map of managers and partitions

  Consumer.committableSource(consumerSettings, Subscriptions.topics("jsonMessages")) //subscribe to the update topic
    .map(msg => {
      chooseProcessor(msg.record.value()) //for each message, decide which command processor should handle it
    })
    .runWith(Sink.ignore) // no further message flow as handled in map


  def chooseProcessor(msg:String): Unit ={
    val choice = commandCount % managerCount //round robin to balence message handling between processors
    managerMap(choice) ! msg //send the command to the chosen processor
    commandCount+=1 //increment the command count
  }

  def initilizeGraph():Unit = {
    for(i <- 0 until childCount){
      val child =  system.actorOf(Props(new GraphPartition(i))) //create graph partitions
      childMap = childMap updated (i,child) //and add to partition map
    }
    childMap.foreach(child => child._2 ! PassPartitionList(childMap))

    for(i <- 0 until managerCount){
      val graphManager = system.actorOf(Props[GraphManager]) //create our graph manager
      managerMap = managerMap updated (i,graphManager) //and add to map
      graphManager ! PassPartitionList(childMap)
    }
    resetLogs() //reset partition logs for testing
  }

  def resetLogs():Unit = {
    "rm -r partitionLogs".!
    "mkdir partitionLogs".!
  }


  // prevent WakeupException on quick restarts of vm
  // No idea why this is needed, but breaks without this
  scala.sys.addShutdownHook{
    println("Terminating... - "+ Instant.now)
    system.terminate()
    Await.result(system.whenTerminated,30 seconds)
  }


  }
