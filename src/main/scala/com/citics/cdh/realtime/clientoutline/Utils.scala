package com.citics.cdh.realtime.clientoutline

import java.text.SimpleDateFormat
import java.util
import java.util.{Calendar, Date}

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.json4s.{DefaultFormats, NoTypeHints}
import org.json4s.native.{Json, Serialization}
import org.slf4j.LoggerFactory
import redis.clients.jedis.{HostAndPort, JedisCluster, JedisPoolConfig}

import scala.collection.{immutable, mutable}
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
  val topicOggCbsrealtime  = "ogg-cbsrealtime"
  val topicOggCbsentrust   = "ogg-cbsentrust"
  val topicOggCbpentrust   = "ogg-cbpentrust"

  //hbase
  val hbaseHosts = "10.23.147.32,10.23.147.33,10.23.147.39"
  val hbasePort = "2181"
  val hbaseTKafkaOffset = "realtime:kafka_offset"
  val hbaseTStkcode = "EDM_REALTIME:STKCODE"
  val hbaseTOptcode = "EDM_REALTIME:OPTCODE"
  val hbaseTPrice   = "EDM_REALTIME:PRICE"
  val hbaseTRealtimeDetails = "realtime:client_outline_realtime_details"
  val hbaseTEntrustDetails  = "realtime:client_outline_entrust_details"
  val hbaseTEntrustMapping  = "realtime:client_outline_entrust_mapping"
  val hbaseTFoudjourDetails  = "realtime:client_outline_fundjour_details"
  val hbaseTCrdtrealtiemDetails = "realtime:client_outline_crdtrealtime_details"
  val hbaseTCrdtentrustDetails = "realtime:client_outline_crdtentrust_details"
  val hbaseTCrdtentrustMapping = "realtime:client_outline_crdtentrust_mapping"
  val hbaseTOptrealtimeDetails = "realtime:client_outline_optrealtime_details"
  val hbaseTOptentrustDetails = "realtime:client_outline_optentrust_details"

  val hbaseTCtstentrustDetails = "realtime:client_outline_ctstentrust_details"
  val hbaseTOfentrustDetails = "realtime:client_outline_ofentrust_details"
  val hbaseTOfentrustMapping = "realtime:client_outline_ofentrust_mapping"
  val hbaseTOtcbookorderDetails = "realtime:client_outline_otcborder_details"
  val hbaseTOtcorderDetails = "realtime:client_outline_otcorder_details"
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
  val redisAggregateTopdealKey  = "realtime:crm:aggregate:realtime:top:staff_id:%s"
  val redisAggregateEntrustKey  = "realtime:crm:aggregate:entrust:init_date:%s:staff_id:%s"
  val redisAggregateFundjourKey = "realtime:crm:aggregate:fundjour:staff_id:%s"
  val redisAggregateFundOutKey  = "realtime:crm:aggregate:fundjour:out:staff_id:%s"
  val redisAggregateStockjourKey = "realtime:crm:aggregate:stockjour:out:staff_id:%s"

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

  def updateRecordsConvert(u: String): Option[mutable.Map[String, (String, String)]] = {
    val rst = new mutable.HashMap[String, (String, String)]()
    val mapper = new ObjectMapper()
    mapper.registerModule(DefaultScalaModule)

    val map = mapper.readValue(u, classOf[immutable.Map[String, Any]])
    if (map.contains("before") && map.contains("after") && map.contains("pos")) {
      val before = map("before").asInstanceOf[Map[String, Any]].map(kv => (kv._1, kv._2.toString))
      val after = map("after").asInstanceOf[Map[String, Any]].map(kv => (kv._1, kv._2.toString))
      val pos = map("pos").asInstanceOf[String]

      for ((k, v) <- before) {
        rst += (k -> (v, after(k)))
      }
      rst += ("pos" -> (pos, pos))
      Some(rst)
    } else {
      None
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
      val datetime = f"${date.toString}%8s ${time.toString.toInt}%09d"
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
      val datetime = f"${date.toString}%8s ${time.toString.toInt}%06d"
      val dt = new SimpleDateFormat("yyyyMMdd HHmmss").parse(datetime)
      val df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
      rst = df.format(dt)
    } catch {
      case ex: Exception => {
        //解析异常 使用当前时间戳
        rst = getSpecDay(0, "yyyy-MM-dd HH:mm:ss")
        logger.warn("parse to 'yyyyMMdd HHmmss' error")
        ex.printStackTrace()
      }
    }
    rst
  }

  def otcOrderTypeConvert = (trd_id: String) => {
    trd_id match {
      case "110" => "认购"
      case "111" => "申购"
      case "112" => "赎回"
      case _ => ""
    }
  }

  def otcTimestampConvert = (time: String) => {
    //转换为yyyy-MM-dd HH:mm:ss格式
    time.replaceFirst(":", " ").substring(0, 19)
  }

  def otcAmtConvert = (amt: String, qty: String) => {
    if (amt.toLong == 0) {
      qty.toLong / 100
    } else {
      amt.toLong /100
    }
  }

  def otcCrtPositionStr = (timeStamp: String, sno: String) => {
    timeStamp.split("\\D").mkString + sno
  }

  def initdateCvt(initDate: String): String = {
    Array(initDate.substring(0,4), initDate.substring(4,6), initDate.substring(6,8)).mkString("-")
  }

  def dateStringAddDays(date: String, num: Int): String = {
    val myformat = new SimpleDateFormat("yyyy-MM-dd")
    var dnow = new Date()
    if(date != ""){
      dnow = myformat.parse(date)}
    val cal = Calendar.getInstance()
    cal.setTime(dnow)
    cal.add(Calendar.DAY_OF_MONTH, num)
    val newday= cal.getTime()
    myformat.format(newday)
  }
}
