package com.citics.cdh.offline.clientoutline

import java.text.SimpleDateFormat
import java.util
import java.util.Calendar

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory}
import org.json4s.DefaultFormats
import org.json4s.native.Json
import redis.clients.jedis.{HostAndPort, JedisPoolConfig}
/**
  * Created by 029188 on 2018-1-8.
  */
object Utils {

  val hiveClientInfo = "bf_ap_crm.tkhxx"
  val hiveEmployInfo = "bf_ap_crm.tryxx"
  val hiveClientRel = "bf_ap_crm.tkhgx"

  val hbaseHosts = "10.23.147.32,10.23.147.33,10.23.147.39"
  val hbasePort = "2181"

  val hbaseTRealtimeDetails = "realtime:client_outline_realtime_details"
  val hbaseTEntrustDetails  = "realtime:client_outline_entrust_details"
  val hbaseTEntrustMapping  = "realtime:client_outline_entrust_mapping"
  val hbaseTFoudjourDetails  = "realtime:client_outline_fundjour_details"
  val hbaseTCrdtrealtiemDetails = "realtime:client_outline_crdtrealtime_details"
  val hbaseTCrdtentrustDetails = "realtime:client_outline_crdtentrust_details"
  val hbaseTOptrealtimeDetails = "realtime:client_outline_optrealtime_details"
  val hbaseTOptentrustDetails = "realtime:client_outline_optentrust_details"
  val hbaseTCtstentrustDetails = "realtime:client_outline_ctstentrust_details"
  val hbaseTOfentrustDetails = "realtime:client_outline_ofentrust_details"
  val hbaseTOfentrustMapping = "realtime:client_outline_ofentrust_mapping"
  val hbaseTOtcbookorderDetails = "realtime:client_outline_otcborder_details"
  val hbaseTOtcorderDetails = "realtime:client_outline_otcorder_details"
  val hbaseTStockjourDetails = "realtime:client_outline_stockjour_details"

  val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.property.clientPort", hbasePort)
  conf.set("hbase.zookeeper.quorum", hbaseHosts)
  def getHbaseConn(): Connection = {
    val conn = ConnectionFactory.createConnection(conf)
    conn
  }

  val jedisConf = new JedisPoolConfig()
  jedisConf.setMaxTotal(100)
  jedisConf.setMaxIdle(50)
  jedisConf.setMinIdle(20)
  jedisConf.setMaxWaitMillis(6 * 1000)
  jedisConf.setTestOnBorrow(true)
  val jedisClusterNodes = new util.HashSet[HostAndPort]()
  jedisClusterNodes.add(new HostAndPort("10.23.152.236", 7000))
  jedisClusterNodes.add(new HostAndPort("10.23.152.236", 7001))
  jedisClusterNodes.add(new HostAndPort("10.23.152.237", 7000))
  jedisClusterNodes.add(new HostAndPort("10.23.152.237", 7001))
  jedisClusterNodes.add(new HostAndPort("10.23.152.239", 7000))
  jedisClusterNodes.add(new HostAndPort("10.23.152.239", 7001))
  jedisClusterNodes.add(new HostAndPort("10.23.152.240", 7000))
  jedisClusterNodes.add(new HostAndPort("10.23.152.240", 7001))
  val redisClientRelKey = "realtime:crm:client_rel:client_id:%s"
  val redisStaffInfoKey = "realtime:crm:staff_info:staff_id:%s"

  def getSpecDay(days: Int, format: String): String = {
    val c = Calendar.getInstance()
    c.add(Calendar.DAY_OF_MONTH, days)
    val dateFormat = new SimpleDateFormat(format)

    dateFormat.format(c.getTime())
  }

  def getUnixStamp(timeStr: String, format: String): Long = {
    val dateFormat = new SimpleDateFormat(format)
    dateFormat.parse(timeStr).getTime / 1000
  }

  def map2Json(map: Map[String, _]): String = {
    Json(DefaultFormats).write(map)
  }
}

