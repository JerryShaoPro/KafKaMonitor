package com.sohu.kafka.offsetapp

import java.util.{Timer, TimerTask, Properties}
import com.sohu.kafka.OffsetGetter.KafkaInfo
import com.sohu.utils.UnfilteredWebApp
import com.sohu.kafka.OffsetGetter
import com.sohu.kafka.OffsetGetter.KafkaInfo
import kafka.utils.{Logging, ZKStringSerializer}
import net.liftweb.json.{NoTypeHints, Serialization}
import net.liftweb.json.Serialization.write
import org.I0Itec.zkclient.ZkClient
import unfiltered.filter.Plan
import unfiltered.request.{GET, Path, Seg}
import unfiltered.response.{JsonContent, Ok, ResponseString}
import com.twitter.util.Time
import net.liftweb.json.JsonAST.JInt

class OWArgs extends OffsetGetterArgs with UnfilteredWebApp.Arguments {
  var retain: Long = _
  var refresh: Int = _

  var dbName: String = "offsetapp"
  lazy val db = new OffsetDB(dbName)
}

/**
 * A webapp to look at consumers managed by kafka and their offsets.
 * User: pierre
 * Date: 1/23/14
 */
object OffsetGetterWeb extends UnfilteredWebApp[OWArgs] with Logging {
  def htmlRoot: String = "/offsetapp"

  val timer = new Timer()
  
  val props = new Properties()
  val in = getClass().getResourceAsStream("/config.properties");
  props.load(in)
  
  def main(args: Array[String]) {
    val parsedArgs = new OWArgs
    parsedArgs.zk = props.getProperty("zk_connect")
    parsedArgs.zkConnectionTimeout = props.getProperty("zk_connection_timeout").toInt
    parsedArgs.zkSessionTimeout = props.getProperty("zk_session_timeout").toInt
    parsedArgs.retain = props.getProperty("db_retain").toLong
    parsedArgs.refresh = props.getProperty("offset_refresh").toInt;
    parsedArgs.port = props.getProperty("port").toInt
    
    start(parsedArgs)
  }
  
  def writeToDb(args: OWArgs) {
    val groups = getGroups(args)
    groups.foreach {
      g =>
        val kafkaInf: KafkaInfo = getInfo(g, args)
        val inf = getInfo(g, args).offsets.toIndexedSeq
        info("inserting ${inf.size}")
        args.db.insetAll(inf)
    }
  }

  /**
   * timer调度器：负责offset记录写入DB和从DB中删除过期的offset记录
   */
  def schedule(args: OWArgs) {
	println(System.currentTimeMillis);
    println(System.currentTimeMillis - args.retain);
    
    timer.scheduleAtFixedRate(new TimerTask() {
      override def run() {
        writeToDb(args)  //插入
      }
    }, 0, args.refresh) 
    timer.scheduleAtFixedRate(new TimerTask() {
      override def run() {
        args.db.emptyOld(System.currentTimeMillis - args.retain) //删除
      }
    }, 0, args.retain)
  }

  /**
   * 创建zkClient对象
   */
  def withZK[T](args: OWArgs)(f: ZkClient => T): T = {
    var zkClient: ZkClient = null
    try {
      zkClient = new ZkClient(args.zk,
        args.zkSessionTimeout,
        args.zkConnectionTimeout,
        ZKStringSerializer)
      f(zkClient)
    } finally {
      if (zkClient != null)
        zkClient.close()
    }
  }

  /**
   * 创建OffsetGetter对象
   */
  def withOG[T](args: OWArgs)(f: OffsetGetter => T): T = withZK(args) {
    zk =>
      var og: OffsetGetter = null
      try {
        og = new OffsetGetter(zk)
        f(og)
      } finally {
        if (og != null) og.close()
      }
  }

  def getInfo(group: String, args: OWArgs): KafkaInfo = withOG(args) {
    _.getInfo(group)
  }

  /**
   * 获取consumer group列表
   */
  def getGroups(args: OWArgs) = withOG(args) {
    _.getGroups
  }

  def getActiveTopics(args: OWArgs) = withOG(args) {
    _.getActiveTopics
  }
  def getTopics(args: OWArgs) = withOG(args) {
    _.getTopics
  }

  def getTopicDetail(topic: String, args: OWArgs) = withOG(args) {
    _.getTopicDetail(topic)
  }

  def getClusterViz(args: OWArgs) = withOG(args) {
    _.getClusterViz
  }

  override def afterStop() {
    timer.cancel()
    timer.purge()
  }

  override def setup(args: OWArgs): Plan = new Plan {
    implicit val formats = Serialization.formats(NoTypeHints)
    args.db.maybeCreate()
    schedule(args)

    def intent: Plan.Intent = {
      case GET(Path(Seg("group" :: Nil))) =>
        JsonContent ~> ResponseString(write(getGroups(args)))
      case GET(Path(Seg("group" :: group :: Nil))) =>
        val info = getInfo(group, args)
        JsonContent ~> ResponseString(write(info)) ~> Ok
      case GET(Path(Seg("group" :: group :: topic :: Nil))) =>
        val offsets = args.db.offsetHistory(group, topic)
        JsonContent ~> ResponseString(write(offsets)) ~> Ok
      case GET(Path(Seg("topiclist" :: Nil))) =>
        JsonContent ~> ResponseString(write(getTopics(args)))
      case GET(Path(Seg("clusterlist" :: Nil))) =>
        JsonContent ~> ResponseString(write(getClusterViz(args)))
      case GET(Path(Seg("topicdetails" :: group :: Nil))) =>
        JsonContent ~> ResponseString(write(getTopicDetail(group, args)))
      case GET(Path(Seg("activetopics" :: Nil))) =>
        JsonContent ~> ResponseString(write(getActiveTopics(args)))
    }
  }
}
