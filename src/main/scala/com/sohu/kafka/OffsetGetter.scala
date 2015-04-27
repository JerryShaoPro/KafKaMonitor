package com.sohu.kafka

import scala.collection._
import com.sohu.kafka.OffsetGetter.{BrokerInfo, KafkaInfo, OffsetInfo}
import com.sohu.kafka.utils.Json
import com.sohu.kafka.utils.BrokerNotAvailableException
import com.sohu.kafka.utils.Broker

import kafka.api.OffsetRequest
import kafka.consumer.SimpleConsumer
import kafka.utils.{Logging, ZkUtils}

import org.I0Itec.zkclient.ZkClient
import org.I0Itec.zkclient.exception.ZkNoNodeException
import com.twitter.util.Time
import org.apache.zookeeper.data.Stat

/**
 * a nicer version of kafka's ConsumerOffsetChecker tool
 * User: pierre
 * Date: 1/22/14
 */

case class Node(name: String, children: Seq[Node] = Seq())
case class TopicDetails(consumers: Seq[ConsumerDetail])
case class ConsumerDetail(name: String)

class OffsetGetter(zkClient: ZkClient) extends Logging {

  private val consumerMap: mutable.Map[Int, Option[SimpleConsumer]] = mutable.Map()

  private def readDataMaybeNull(client: ZkClient, path: String): (Option[String], Stat) = {
    val stat: Stat = new Stat()
    val dataAndStat = try {
                        (Some(client.readData(path, stat)), stat)
                      } catch {
                        case e: ZkNoNodeException =>
                          (None, stat)
                        case e2: Throwable => throw e2
                      }
    dataAndStat
  }
  
  def getConsumer(bid: Int): Option[SimpleConsumer] = {
    try {
      readDataMaybeNull(zkClient, ZkUtils.BrokerIdsPath + "/" + bid) match {
        case (Some(brokerInfoString), _) =>
          val brokerInfo = brokerInfoString.split(":")
          val host = brokerInfo(1)
          val port = brokerInfo(2).toInt
          Some(new SimpleConsumer(host, port, 10000, 100000))
        case _ =>
          throw new BrokerNotAvailableException("Broker id %d does not exist".format(bid))
      }
    } catch {
      case t: Throwable =>
        error("Could not parse broker info", t)
        None
    }
  }

  private def readData(client: ZkClient, path: String): (String, Stat) = {
    val stat: Stat = new Stat()
    val dataStr: String = client.readData(path, stat)
    (dataStr, stat)
  }
  
  
  val BrokerTopicsPath = "/brokers/topics"
  val consumersPath = "/consumers"
  
  private def getTopicPath(topic: String): String = {
    BrokerTopicsPath + "/" + topic
  }
  
  private def getTopicPartitionsPath(topic: String): String = {
    getTopicPath(topic) + "/partitions"
  }
  
  private def getTopicPartitionPath(topic: String, partitionId: Int): String =
    getTopicPartitionsPath(topic) + "/" + partitionId
  
  private def getTopicPartitionLeaderAndIsrPath(topic: String, partitionId: Int): String =
    getTopicPartitionPath(topic, partitionId) + "/" + "state"
  
   private def getLeaderForPartition(zkClient: ZkClient, topic: String, partition: Int): Option[Int] = {
    val leaderAndIsrOpt = readDataMaybeNull(zkClient, getTopicPartitionLeaderAndIsrPath(topic, partition))._1
    leaderAndIsrOpt match {
      case Some(leaderAndIsr) =>
        Json.parseFull(leaderAndIsr) match {
          case Some(m) =>
            Some(m.asInstanceOf[Map[String, Any]].get("leader").get.asInstanceOf[Int])
          case None => None
        }
      case None => None
    }
  }
  
  /**
   * 处理 /consumers/$group/offsets/$topic/$partition 下的数据
   * 处理 /consumers/$group/owners/$topic/$partition 下的数据
   */
  private def processPartition(group: String, topic: String, pid: String): Option[OffsetInfo] = {
    try {
      val (offset, stat: Stat) = readData(zkClient, consumersPath + "/" + group + "/offsets/" + topic + "/" + pid)
      val (owner, _) = readDataMaybeNull(zkClient, consumersPath + "/" + group + "/owners/" + topic + "/" + pid)

      val pidd = pid.split("-")(1).toInt
      Option[Int](pid.split("-")(0).toInt) match {
      	case Some(bid) =>
          val consumerOpt = consumerMap.getOrElseUpdate(bid, getConsumer(bid))
          consumerOpt map {
            consumer =>
              val logSize = consumer.getOffsetsBefore(topic, pidd, -1L, 1)(0) //返回最大的一个
              
              OffsetInfo(group = group,
                topic = topic,
                partition = pid,
                offset = offset.toLong,
                logSize = logSize,
                owner = owner)
          }
        case None =>
          error("No broker for partition %s - %s".format(topic, pid))
          None
      }
    } catch {
      case t: Throwable =>
        error("Could not parse partition info. group: [$group] topic: [$topic]", t)
        None
    }
  }

  /**
   * 
   */
  private def processTopic(group: String, topic: String): Seq[OffsetInfo] = {
    val pidMap = ZkUtils.getPartitionsForTopics(zkClient, List(topic).iterator) //获取topic对应的partition数据
    for {
      partitions <- pidMap.get(topic).toSeq //获取该topic的partition
      pid <- partitions.sorted //排序parition
      info <- processPartition(group, topic, pid) //
    } yield info
  }

  private def brokerInfo(): Iterable[BrokerInfo] = {
    for {
      (bid, consumerOpt) <- consumerMap
      consumer <- consumerOpt
    } yield BrokerInfo(id = bid, host = consumer.host, port = consumer.port)
  }

  /**
   * 获取zk中/consumers/$group/offsets
   */
  private def offsetInfo(group: String, topics: Seq[String] = Seq()): Seq[OffsetInfo] = {

    val topicList = if (topics.isEmpty) { //先获取topics
      try {
        ZkUtils.getChildren(
          zkClient, consumersPath + "/" + group + "/offsets").toSeq
      } catch {
        case _: ZkNoNodeException => Seq()
      }
    } else {
      topics
    }
    topicList.sorted.flatMap(processTopic(group, _)) //处理每个topic
  }

  /**
   * 获取brokers和offsets
   */
  def getInfo(group: String, topics: Seq[String] = Seq()): KafkaInfo = {
    val off = offsetInfo(group, topics)
    val brok = brokerInfo()
    KafkaInfo(
      brokers = brok.toSeq,
      offsets = off
    )
  }

  /**
   * 获取zk中/consumer路径下的数据列表
   */
  def getGroups: Seq[String] = {
    ZkUtils.getChildren(zkClient, ZkUtils.ConsumersPath)
  }


  /**
   * returns details for a given topic such as the active consumers pulling off of it
   * @param topic
   * @return
   */
  def getTopicDetail(topic: String): TopicDetails = {
    val topicMap = getActiveTopicMap

    if(topicMap.contains(topic)) {
      TopicDetails(topicMap(topic).map(consumer => {
        ConsumerDetail(consumer.toString)
      }).toSeq)
    } else {
      TopicDetails(Seq(ConsumerDetail("Unable to find Active Consumers")))
    }
  }

  def getTopics: Seq[String] = {
    ZkUtils.getChildren(zkClient, ZkUtils.BrokerTopicsPath).sortWith(_ < _)
  }


  /**
   * returns a map of active topics-> list of consumers from zookeeper, ones that have IDS attached to them
   *
   * @return
   */
  def getActiveTopicMap: Map[String, Seq[String]] = {
    val topicMap: mutable.Map[String, Seq[String]] = mutable.Map()

    ZkUtils.getChildren(zkClient, ZkUtils.ConsumersPath).foreach(group => {
      ZkUtils.getConsumersPerTopic(zkClient, group).keySet.foreach(key => {
        if (!topicMap.contains(key)) {
          topicMap.put(key, Seq(group))
        } else {
          topicMap.put(key, topicMap(key) :+ group)
        }
      })
    })
    topicMap.toMap
  }

  def getActiveTopics: Node = {
    val topicMap = getActiveTopicMap

    Node("ActiveTopics", topicMap.map {
      case (s: String, ss: Seq[String]) => {
        Node(s, ss.map(consumer => Node(consumer)))

      }
    }.toSeq)
  }

  private def getBrokerInfo(zkClient: ZkClient, brokerId: Int): Option[Broker] = {
    readDataMaybeNull(zkClient, ZkUtils.BrokerIdsPath + "/" + brokerId)._1 match {
      case Some(brokerInfo) => Some(Broker.createBroker(brokerId, brokerInfo))
      case None => None
    }
  }
  
  private def getAllBrokersInCluster(zkClient: ZkClient): Seq[Broker] = {
    val brokerIds = ZkUtils.getChildrenParentMayNotExist(zkClient, ZkUtils.BrokerIdsPath).sorted
    brokerIds.map(_.toInt).map(getBrokerInfo(zkClient, _)).filter(_.isDefined).map(_.get)
  }
  
  def getClusterViz: Node = {
    val clusterNodes = getAllBrokersInCluster(zkClient).map((broker) => {
        Node(broker.getConnectionString(), Seq())
    })
    Node("KafkaCluster", clusterNodes)
  }

  def close() {
    for (consumerOpt <- consumerMap.values) {
      consumerOpt match {
        case Some(consumer) => consumer.close()
        case None => // ignore
      }
    }
  }

}

object OffsetGetter {
  case class KafkaInfo(brokers: Seq[BrokerInfo], offsets: Seq[OffsetInfo])

  case class BrokerInfo(id: Int, host: String, port: Int)

  case class OffsetInfo(group: String,
                        topic: String,
                        partition: String,
                        offset: Long,
                        logSize: Long,
                        owner: Option[String]
                        ) {
    val lag = logSize - offset
  }
}