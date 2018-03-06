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

    hvc.udf.register("getStaffId", getStaffId)
    val i = args(0)

    i match {
      case "0" => calCustRelToRedis(hvc)
      case "1" => calStaffInfoToRedis(hvc)
      case _ => {}
    }
  }

  def calStaffInfoToRedis(hvc: HiveContext): Unit = {

    val df = hvc.sql("select getStaffId(id,0) as staff_id, COALESCE(ryxm,'') as staff_name " +
                     s"from ${Utils.hiveEmployInfo} " +
                     "group by id, ryxm")
    val df1 = hvc.sql("select getStaffId(id,1) as staff_id, COALESCE(ryxm,'') as staff_name " +
                      s"from ${Utils.hiveEmployInfo} " +
                      "group by id, ryxm")

    val df2 = df.unionAll(df1)
    df2.foreachPartition(iter => {

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
          jedisCluster.hincrBy(key, "cbsrealtime_count", 0)
          jedisCluster.hincrBy(key, "cbsentrust_count", 0)
          jedisCluster.hincrBy(key, "cbpentrust_count", 0)

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

    val staff_client_rln = hvc.sql("select a.yskhh as client_id, " +
                                   "COALESCE(a.khxm,'') as client_name, " +
                                   "getStaffId(b.ryxx, 0) as staff_id, " +
                                   "COALESCE(c.ryxm,'') as staff_name " +
                                   s"from ${Utils.hiveClientInfo} a, " +
                                   s"${Utils.hiveClientRln} b, " +
                                   s"${Utils.hiveEmployInfo} c " +
                                   "where a.id = b.khh and " +
                                   "b.ryxx = c.id and " +
                                   "a.yskhh is not null and " +
                                   "b.ryxx is not null " +
                                   "group by a.yskhh, a.khxm, b.ryxx, c.ryxm").cache()

    val group_leaders = hvc.sql(s"select distinct supr_empe_id from ${Utils.hiveGroupRln} " +
                                "where supr_empe_id is not null")
    group_leaders.registerTempTable("group_leaders")

    val group_client_rln = hvc.sql("select gcl.src_stm_cust_no as client_id, " +
                                   "COALESCE(cif.client_name,'') as client_name, " +
                                   "getStaffId(gcl.empe_id, 1) as staff_id, " +
                                   "COALESCE(eif.ryxm,'') as staff_name " +
                                   s"from ${Utils.hiveGroupClientRln} gcl, " +
                                   "group_leaders gld, " +
                                   s"${Utils.hiveHsClient} cif, " +
                                   s"${Utils.hiveEmployInfo} eif " +
                                   "where gcl.empe_id = gld.supr_empe_id and " +
                                   "gcl.src_stm_cust_no = cif.client_id and " +
                                   "gcl.empe_id = eif.id and " +
                                   "gcl.src_stm_cust_no is not null " +
                                   "group by gcl.src_stm_cust_no, cif.client_name, gcl.empe_id, eif.ryxm").cache()

    val df = staff_client_rln.unionAll(group_client_rln)

    val rdd = df.map(r => (r.getAs[String]("client_id"), (r.getAs[String]("client_name"),
                           r.getAs[String]("staff_id").toString, r.getAs[String]("staff_name")))).groupByKey()

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
//          println(r)
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

  def getStaffId = (id: Long, t: Int) => {
    if(t == 0)
      id.toString
    else
      "g" + id.toString
  }
}
