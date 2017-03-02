package kafka

import java.util.{Date, Properties, Random}

import kafka.javaapi.producer.Producer
import kafka.producer.KeyedMessage
import kafka.producer.ProducerConfig
import twitter4j._
import twitter4j.conf.ConfigurationBuilder

object TwitterToKafka extends App{
  val props:Properties = new Properties()
  props.put("metadata.broker.list", "localhost:9092")
  props.put("serializer.class", "kafka.serializer.StringEncoder")
  props.put("partitioner.class", "kafka.SimplePartitioner")
  props.put("request.required.acks", "1")

  val config:ProducerConfig = new ProducerConfig(props)
  val configurationBuilder: ConfigurationBuilder = new ConfigurationBuilder
  configurationBuilder.setOAuthConsumerKey("hY6rgk7n387pw48XrRTodw5S5")
    .setOAuthConsumerSecret("LSF41BtAOFmS1k8XmUHvXEOPHcNIMPIGI9p2oDTk7qNZxGScfH")
    .setOAuthAccessToken("1565672916-53qrz4XRuof7HcyiqKqQ9KoSSQ3hXty1WRjNbEK")
    .setOAuthAccessTokenSecret("AEqxYdIQ22JMwwb2rWFhcxur7LY8Xib1it8UBv5IGU6LC")

  val twitterStream: TwitterStream = new TwitterStreamFactory(configurationBuilder.build).getInstance
  val producer = new Producer[String,String](config)
  val listener: StatusListener = new StatusListener() {
    def onStatus(status: Status) {
      val rnd = new Random
      val ip = "192.168.2." + rnd.nextInt(255)

      val data = new KeyedMessage[String,String]("page_visits",ip,status.getText)
      producer.send(data)

    }
    def onDeletionNotice(statusDeletionNotice: StatusDeletionNotice){}
    def onTrackLimitationNotice(numberOfLimitedStatuses: Int){}
    def onScrubGeo(userId: Long, upToStatusId: Long) {}
    def onStallWarning(warning: StallWarning) {}
    def onException(ex: Exception) {ex.printStackTrace()}
  }
  twitterStream.addListener(listener)
  twitterStream.sample()
  //producer.close
}
