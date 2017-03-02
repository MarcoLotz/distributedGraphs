package kafka

import kafka.producer.Partitioner
import kafka.utils.VerifiableProperties

class SimplePartitioner(props:VerifiableProperties) extends Partitioner {


  def partition(key: Any, a_numPartitions: Int): Int = {
    var partition: Int = 0
    val stringKey: String = key.asInstanceOf[String]
    val offset: Int = stringKey.lastIndexOf('.')
    if (offset > 0) partition = stringKey.substring(offset + 1).toInt % a_numPartitions
    partition
  }

}
