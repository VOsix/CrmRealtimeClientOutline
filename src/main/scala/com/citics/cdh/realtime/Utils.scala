package com.citics.cdh.realtime

import java.text.SimpleDateFormat
import java.util
import java.util.Calendar

import org.json4s.{DefaultFormats, NoTypeHints}
import org.json4s.native.{Json, Serialization}
import org.json4s.native.Serialization.write
import org.slf4j.LoggerFactory
import redis.clients.jedis.{HostAndPort, JedisPoolConfig}

import scala.collection.mutable
import scala.util.parsing.json.JSON

/**
  * Created by 029188 on 2017-11-30.
  */
object Utils {

  val logger = LoggerFactory.getLogger(getClass)

  //kafka
  val brokerList = "kk01.cshadoop.com:9092,kk02.cshadoop.com:9092,nn03.cshadoop.com:9092"
  val topicOggRealtime     = "ogg-realtime"
  val topicOggEntrust      = "ogg-entrust"
  val topicOggFoudjour     = "ogg-fundjour"
  val topicOggCrdtrealtime = "ogg-crdtrealtime"
  val topicOggCrdtentrust  = "ogg-crdtentrust"
  val topicOggOptrealtime  = "ogg-optrealtime"
  val topicOggOptentrust   = "ogg-optentrust"
  val topicOggOfentrust    = "ogg-ofentrust"
  val topicOggOtcbookorder = "ogg-otcbookorder"
  val topicOggOtcorder     = "ogg-otcorder"
  val topicOggCtstentrust  = "ogg-ctstentrust"
  val topicOggStockjour    = "ogg-stockjour"

  //hbase
  val hbaseHosts = "10.23.147.32,10.23.147.33,10.23.147.39"
  val hbasePort = "2181"
  val hbaseTKafkaOffset     = "realtime:kafka_offset"
  val hbaseTRealtimeDetails = "realtime:client_outline_realtime_details"
  val hbaseTEntrustDetails  = "realtime:client_outline_entrust_details"
  val hbaseTFoudjourDetails  = "realtime:client_outline_fundjour_details"
  val hbaseTCrdtrealtiemDetails = "realtime:client_outline_crdtrealtime_details"
  val hbaseTCrdtentrustDetails = "realtime:client_outline_crdtentrust_details"
  val hbaseTOptrealtimeDetails = "realtime:client_outline_optrealtime_details"
  val hbaseTOptentrustDetails = "realtime:client_outline_optentrust_details"

  val hbaseTOfentrustDetails = "realtime:client_outline_ofentrust_details"
  val hbaseTOtcbookorderDetails = "realtime:client_outline_otcborder_details"
  val hbaseTOtcorderDetails = "realtime:client_outline_otcorder_details"
  val hbaseTCtstentrustDetails = "realtime:client_outline_ctstentrust_details"
  val hbaseTStockjourDetails = "realtime:client_outline_stockjour_details"


  //hive
  val hiveStockCode = "bf_hs_user.stkcode"
  val hiveSystemDict = "bf_hs_user.sysdictionary"
  val hiveOptCode = "bf_hs_user.optcode"
  val hiveBusFlag = "bf_hs_user.businflag"
  val hiveBranch  = "bf_hs_user.allbranch"
  val hiveBankArg = "bf_hs_asset.bankarg"

  //redis
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
  val redisAggregateRealtimeKey = "realtime:crm:aggregate:realtime:staff_id:%s"
  val redisAggregateTopdealKey  = "realtime:crm:aggregate:realtime:top10:staff_id:%s"
  val redisAggregateEntrustKey  = "realtime:crm:aggregate:entrust:staff_id:%s"
  val redisAggregateFundjourKey = "realtime:crm:aggregate:fundjour:staff_id:%s"
  val redisAggregateFundOutKey  = "realtime:crm:aggregate:fundjour:out:staff_id:%s"


  val insertOpt = "\"op_type\":\"I\""
  val updateOpt = "\"op_type\":\"U\""
  val insertPattern = """\{[^\{]+(\{.*\})\}""".r
  val updatePattern = """\{[^\{]+(\{.*\})[^\{]+(\{.*\})\}""".r

  var stkcodesLastDate: String = ""

  def insertRecordsConvert(i: String): Option[String] = {
    i match {
      case insertPattern(s) => Some(s)
      case _ => None
    }
  }

  def updateRecordsConvert(u: String): Option[String] = {
    u match {
      case updatePattern(bf, af) => {
        val before = json2Map(bf)
        val after = json2Map(af)
        val map = new mutable.HashMap[String, List[Any]]()
        for((k,v) <- before.iterator) {
          map += (k -> List(v, after(k)))
        }
        implicit val formats = Serialization.formats(NoTypeHints)
        Some(write(map))
      }
      case _ => None
    }
  }

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

  def json2Map(json: String): Map[String, Any] = {
    val result = JSON.parseFull(json)
    result match {
      case Some(map: Map[String, Any]) => map
      case _ => throw new Exception("json Parsing failed: " + json)
    }
  }

  def map2Json(map: Map[String, _]): String = {
    Json(DefaultFormats).write(map)
  }

  def concatDateTime = (date: Any, time: Any) => {

    var rst: String = ""
    try {
      val datetime = String.format("%8s %9s", date.toString, time.toString)
      val dt = new SimpleDateFormat("yyyyMMdd HHmmssSSS").parse(datetime)
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      rst = df.format(dt)
    } catch {
      case ex: Exception => {
        //解析异常 使用当前时间戳
        rst = getSpecDay(0, "yyyy-MM-dd HH:mm:ss")
        logger.warn("parse to 'yyyyMMdd HHmmssSSS' error")
        ex.printStackTrace()
      }
    }
    rst
  }

  def concatDateTime2 = (date: Any, time: Any) => {

    var rst: String = ""
    try {
      val datetime = String.format("%8s %6s", date.toString, time.toString)
      val dt = new SimpleDateFormat("yyyyMMdd HHmmss").parse(datetime)
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      rst = df.format(dt)
    } catch {
      case ex: Exception => {
        //解析异常 使用当前时间戳
        rst = getSpecDay(0, "yyyy-MM-dd HH:mm:ss")
        logger.warn("{} parse to 'yyyyMMdd HHmmss' error")
        ex.printStackTrace()
      }
    }
    rst
  }



}
