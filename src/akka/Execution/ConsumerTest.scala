package akka.Execution

import java.time.Instant

import akka.GraphActors._
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

  var commandCount:Int = 0 //unique ID for commands coming through system
  val commandProcessorCount = 4 //Number of Command Processors (Incoming String to Case class)

  val graphManager = system.actorOf(Props[GraphManager]) //create our graph manager
  graphManager ! InitilizeGraph(3) //Initilize Graph manager to spawn children (graph partitions)

  var commandProcessorMap = Map[Int,ActorRef]() //create map of command processors
  for(i <- 0 until commandProcessorCount){
    val processor =  system.actorOf(Props(new CommandProcessor(graphManager))) //Create Command processors
    commandProcessorMap = commandProcessorMap updated (i,processor) //and add to map
  }

  val consumerSettings = ConsumerSettings(system, new ByteArrayDeserializer, new StringDeserializer) //pass actor system and deserializer for message K/V
    .withBootstrapServers("localhost:9092") //specify where the bootstrap server for kafka is
    .withGroupId("akka-stream-kafka-test") //group id for the consumer, note that offsets are always committed for a given consumer group
    .withProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest") // tuning params

  Consumer.committableSource(consumerSettings, Subscriptions.topics("jsonMessages")) //subscribe to the update topic
    .map(msg => {
      chooseProcessor(msg.record.value()) //for each message, decide which command processor should handle it
    })
    .runWith(Sink.ignore) // no further message flow as handled in map


  // prevent WakeupException on quick restarts of vm
  // No idea why this is needed, but breaks without this
  scala.sys.addShutdownHook{
    println("Terminating... - "+ Instant.now)
    system.terminate()
    Await.result(system.whenTerminated,30 seconds)
  }

  def chooseProcessor(msg:String): Unit ={
    val choice = commandCount % commandProcessorCount //round robin to balence message handling between processors
    commandProcessorMap(choice) ! msg //send the command to the chosen processor
    commandCount+=1 //increment the command count
  }


  }
