package com.citics.cdh.offline.clientoutline

import java.util
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import redis.clients.jedis.JedisCluster

/**
  * Created by 029188 on 2018-1-8.
  */

object SyncDataToRedis {

  val conf = new SparkConf().setAppName("crmClientOutline_syncDataToRedis")
  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(conf)
    val hvc = new HiveContext(sc)
    import hvc.implicits._
    sc.setLogLevel("WARN")

    val i = args(0)

    i match {
      case "0" => calCustRelToRedis(hvc)
      case "1" => calStaffInfoToRedis(hvc)
      case _ => {}
    }
  }

  def calStaffInfoToRedis(hvc: HiveContext): Unit = {

    val df = hvc.sql(s"select id as staff_id, COALESCE(ryxm,'') as staff_name from ${Utils.hiveEmployInfo} " +
                     s"group by id, ryxm")

    df.foreachPartition(iter => {

      var jedisCluster: JedisCluster = null
      try {
        jedisCluster = new JedisCluster(Utils.jedisClusterNodes, 2000, 100, Utils.jedisConf)

        for (r <- iter) {

          val staff_id = r(0).toString
          val staff_name = r(1).toString

          val key = String.format(Utils.redisStaffInfoKey, staff_id)
          val map = new util.HashMap[String, String]()
          map.put("staff_name", staff_name)
          jedisCluster.hmset(key, map)

          //明细笔数记录
          jedisCluster.hincrBy(key, "realtime_count", 0)
          jedisCluster.hincrBy(key, "entrust_count", 0)
          jedisCluster.hincrBy(key, "fundjour_count", 0)
          jedisCluster.hincrBy(key, "stockjour_count", 0)
          jedisCluster.hincrBy(key, "crdtrealtime_count", 0)
          jedisCluster.hincrBy(key, "crdtentrust_count", 0)
          jedisCluster.hincrBy(key, "optreal_count", 0)
          jedisCluster.hincrBy(key, "optentrust_count", 0)
          jedisCluster.hincrBy(key, "ofentrust_count", 0)
          jedisCluster.hincrBy(key, "ctstentrust_count", 0)
          jedisCluster.hincrBy(key, "otcbookorder_count", 0)
          jedisCluster.hincrBy(key, "otcorder_count", 0)

          //第二天零点失效
          jedisCluster.expireAt(key, Utils.getUnixStamp(Utils.getSpecDay(1, "yyyy-MM-dd"), "yyyy-MM-dd"))
        }
      } catch {
        case ex: Exception => {
          ex.printStackTrace()
          throw ex
        }
      } finally {
        if (jedisCluster != null)
          jedisCluster.close()
      }
    })
  }

  case class StaffInfo(id: String, name: String) {
  }

  def calCustRelToRedis(hvc: HiveContext): Unit = {

    val df = hvc.sql("select a.yskhh as client_id, COALESCE(a.khxm,'') as client_name, " +
                     "b.ryxx as staff_id, COALESCE(c.ryxm,'') as staff_name " +
                     s"from ${Utils.hiveClientInfo} a, " +
                     s"${Utils.hiveClientRel} b, " +
                     s"${Utils.hiveEmployInfo} c " +
                     s"where a.id = b.khh and " +
                     s"b.ryxx = c.id and " +
                     s"a.yskhh is not null and " +
                     s"b.ryxx is not null " +
                     s"group by a.yskhh, a.khxm, b.ryxx, c.ryxm")

    val rdd = df.map(r => (r.getAs[String]("client_id"), (r.getAs[String]("client_name"),
                           r.getAs[Long]("staff_id").toString, r.getAs[String]("staff_name")))).groupByKey()

    rdd.map(x => {

      val mapper = new ObjectMapper()
      mapper.registerModule(DefaultScalaModule)

      val client_id = x._1
      var client_name = ""
      val staffList = new util.ArrayList[StaffInfo]()
      for (i <- x._2) {
        if (client_name != i._1) {
          if (client_name == "")
            client_name = i._1
          else
            logger.warn(s"客户号: ${client_id} 对应多个客户姓名: ${client_name} ${i._1}")
        }
        staffList.add(new StaffInfo(i._2, i._3))
      }
      (client_id, client_name, mapper.writeValueAsString(staffList))
    }).foreachPartition(iter => {
      //写入redis
      var jedisCluster: JedisCluster = null
      try {
        jedisCluster = new JedisCluster(Utils.jedisClusterNodes, 2000, 100, Utils.jedisConf)
        for (r <- iter) {
          val client_id = r._1
          val client_name = r._2
          val staff_list = r._3

          val key = String.format(Utils.redisClientRelKey, client_id)
          val map = new util.HashMap[String, String]()
          map.put("client_name", client_name)
          map.put("staff_list", staff_list)
          jedisCluster.hmset(key, map)
          //第二天零点失效
          jedisCluster.expireAt(key, Utils.getUnixStamp(Utils.getSpecDay(1, "yyyy-MM-dd"), "yyyy-MM-dd"))
        }
      } catch {
        case ex: Exception => {
          ex.printStackTrace()
          throw ex
        }
      } finally {
        if (jedisCluster != null)
          jedisCluster.close()
      }
    })
  }

}
