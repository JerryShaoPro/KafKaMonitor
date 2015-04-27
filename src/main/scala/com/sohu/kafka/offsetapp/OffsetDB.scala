package com.sohu.kafka.offsetapp

import com.sohu.kafka.OffsetGetter.OffsetInfo
import com.sohu.kafka.offsetapp.OffsetDB.{DbOffsetInfo, OffsetHistory, OffsetPoints}
import org.scalaquery.ql.basic.{BasicTable => Table}
import org.scalaquery.ql.extended._
import org.scalaquery.ql.TypeMapper._
import org.scalaquery.ql._
import org.scalaquery.session._
import org.scalaquery.session.Database.threadLocalSession
import org.scalaquery.meta.MTable
import org.scalaquery.ql.basic.BasicDriver.Implicit._
import com.twitter.util.Time

import java.sql.DriverManager

/**
 * Tools to store offsets in a DB
 * 
 * TODO: 1为表加索引; 2设定id字段为主键并自增
 * 
 * User: andrews
 * Date: 1/27/14
 */
class OffsetDB(dbfile: String) {

  val database = Database.forURL("jdbc:sqlite:" + dbfile + ".db", 
      driver = "org.sqlite.JDBC") 

  class Offset extends Table[(String, String, String, Long, Long, Option[String], Long)]("OFFSETS") {
    val group = column[String]("group")
    val topic = column[String]("topic")
    val partition = column[String]("partition")
    val offset = column[Long]("offset")
    val logSize = column[Long]("log_size")
    val owner = column[Option[String]]("owner")
    val timestamp = column[Long]("timestamp")
    
    def * = (group ~ topic ~ partition ~ offset ~ logSize ~ owner ~ timestamp)
    
    def idx = index("idx", group ~ topic) //联合索引
    
    def tidx = index("idx_time", timestamp)
  
    def forHistory = (timestamp, partition, owner, offset, logSize)
  }

  val offsets = new Offset
  
  /**
   * 插入一条记录
   */
  def insert(timestamp: Long, info: OffsetInfo) {
    database.withSession {
        offsets insert (info.group, info.topic, info.partition, info.offset, info.logSize, info.owner, timestamp)
    }
  }

  /**
   * 批量插入多条记录
   */
  def insetAll(info: IndexedSeq[OffsetInfo]) {
    val now = System.currentTimeMillis
    database.withSession {
      info.foreach(i => offsets insert (i.group, i.topic, i.partition, i.offset, i.logSize, i.owner, now))
    }
  }

  /**
   * 删除过期记录
   */
  def emptyOld(since: Long) {
//    database.withSession {
//        offsets.where(_.timestamp < since).delete //此方式不支持，可能是scalaQuery版本太低
//    }
    
//    val delRs = for { 		//此方式也有问题，报sqlite游标不允许更新
//    	rec <- offsets if rec.timestamp < since
//    } yield rec
//	delRs.mutate(_.delete)	
    
    //算了，那就不用ORM了，直接使用jdbc，shit！！！还是jdbc牛x
    Class.forName("org.sqlite.JDBC")
	val conn = DriverManager.getConnection("jdbc:sqlite:offsetapp.db")
    val deleteStatement = conn.prepareStatement("delete from OFFSETS where timestamp < ?")
	try {
		deleteStatement.setLong(1, since)
		deleteStatement.executeUpdate()
	} finally deleteStatement.close()
  }

  /**
   * 显示历史数据
   */
  def offsetHistory(group: String, topic: String): OffsetHistory = database.withSession {
    val d = for {
      c <- offsets if c.group === group && c.topic === topic
      _ <- Query orderBy c.timestamp
    } yield c.timestamp ~ c.partition ~ c.owner ~ c.offset ~ c.logSize
    
    var offsetPointsList: List[OffsetPoints] = List()
    for (di <- d) {
      val offsetPoints = OffsetPoints(di._1, di._2, di._3, di._4, di._5)
      offsetPointsList = offsetPoints :: offsetPointsList
    }
    
    OffsetHistory(group, topic, offsetPointsList.reverse)
  }

  /**
   * 若果表不存在，则创建
   */
  def maybeCreate() {
    database.withSession {
        if (MTable.getTables("OFFSETS").list().isEmpty) {
          offsets.ddl.create
        }
    }
  }
}

/**
 * offset数据库
 */
object OffsetDB {
  /**
   * kafkaOffset 某时刻的信息描述类
   */
  case class OffsetPoints(timestamp: Long, partition: String, owner: Option[String], offset: Long, logSize: Long)

  /**
   * 特定(group, topic)的 kafkaOffset 信息集描述类
   */
  case class OffsetHistory(group: String, topic: String, offsets: Seq[OffsetPoints])
//  case class OffsetHistory(group: String, topic: String, offsets: Seq[(Long, String, Option[String], Long, Long)])

  /**
   * kafkaOffset 数据表中存储信息描述类
   */
  case class DbOffsetInfo(timestamp: Long, offset: OffsetInfo)

  /**
   * DbOffsetInfo类的实例化对象，提供工厂方法
   */
  object DbOffsetInfo {
    /**
     * DbOffsetInfo的实例化工厂方法
     */
    def parse(in: (String, String, String, Long, Long, Option[String], Long)): DbOffsetInfo = {
      val (group, topic, partition, offset, logSize, owner, timestamp) = in
      DbOffsetInfo(timestamp, OffsetInfo(group, topic, partition, offset, logSize, owner))
    }

    /**
     * DbOffsetInfo的反实例化工厂方法
     */
    def unparse(in: DbOffsetInfo): Option[(String, String, String, Long, Long, Option[String], Long)] = Some(
      in.offset.group,
      in.offset.topic,
      in.offset.partition,
      in.offset.offset,
      in.offset.logSize,
      in.offset.owner,
      in.timestamp
    )
  }
}
