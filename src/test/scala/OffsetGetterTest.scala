import org.I0Itec.zkclient.ZkClient
import kafka.consumer.SimpleConsumer

object OffsetGetterTest {
  
  def main(args: Array[String]): Unit = {
    val consumer = new ZkClient("10.11.152.97:2181")
    
    val simpleConsumer = new SimpleConsumer("10.11.152.66", 9092, 10000, 100000)
    val logSize = simpleConsumer.getOffsetsBefore("sohuwl-pv", 0, -1L, 1)(0) //返回最大的一个
    println(logSize)
  }
  
}
