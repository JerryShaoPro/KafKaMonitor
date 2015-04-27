package com.sohu.kafka.offsetapp

import kafka.utils.ZKStringSerializer
import org.I0Itec.zkclient.ZkClient
import com.sohu.kafka.OffsetGetter

/**
 * 获取zk中相关consumer/topic的offset的类
 */
class OffsetGetterArgsWGT extends OffsetGetterArgs {
  var group: String = _

  var topics: Seq[String] = Seq()
}

/**
 * 从zk中获取offset需要携带的参数的类
 */
class OffsetGetterArgs {
  var zk: String = _
  var zkSessionTimeout: Int = _ 
  var zkConnectionTimeout: Int = _ 
}

/**
 * 获取offset的具体对象
 * User: pierre
 * Date: 1/22/14
 */
object OffsetGetterApp {
  def main(args: OffsetGetterArgsWGT) {
    var zkClient: ZkClient = null
    var og: OffsetGetter = null
    try {
      zkClient = new ZkClient(args.zk,
        args.zkSessionTimeout,
        args.zkConnectionTimeout,
        ZKStringSerializer)
      og = new OffsetGetter(zkClient)
      val i = og.getInfo(args.group, args.topics)

      if (i.offsets.nonEmpty) {
        println("%-15s\t%-40s\t%-3s\t%-15s\t%-15s\t%-15s\t%s".format("Group", "Topic", "Pid", "Offset", "logSize", "Lag", "Owner"))
        i.offsets.foreach {
          info =>
            println("%-15s\t%-40s\t%-3s\t%-15s\t%-15s\t%-15s\t%s".format(info.group, info.topic, info.partition, info.offset, info.logSize, info.lag,
              info.owner.getOrElse("none")))
        }
        println()
        println("Brokers")
        i.brokers.foreach {
          b =>
            println("${b.id}\t${b.host}:${b.port}")
        }
      } else {
        System.err.println("no topics for group ${args.group}")
      }

    }
    finally {
      if (og != null) og.close()
      if (zkClient != null)
        zkClient.close()
    }
  }
}
