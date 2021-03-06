package com.citics.cdh.realtime.clientoutline

import java.util

import com.citics.cdh.kafkautils.KafkaReader
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Get, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import redis.clients.jedis.JedisCluster

import scala.collection.JavaConversions._
import scala.collection.immutable

/**
  * Created by 029188 on 2017-12-5.
  */
object OtcCbsDetails {

  val conf = new SparkConf().setAppName("crmClientOutline_otccbsDetail")
  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(20))
    val hvc = new HiveContext(sc)

    sc.setLogLevel("WARN")

    otcorderPrc(sc, ssc, hvc)
    otcbookorderPrc(sc, ssc, hvc)
    cbsentrustPrc(sc, ssc, hvc)
    cbsrealtimePrc(sc, ssc, hvc)

    ssc.start()
    ssc.awaitTermination()
  }

  def otcorderPrc(sc: SparkContext, ssc: StreamingContext, hvc: HiveContext): Unit = {
    val kafkaReader = new KafkaReader[String, String, StringDecoder, StringDecoder](ssc, Utils.brokerList, Utils.topicOggOtcorder,
                                                                                    Utils.hbaseTKafkaOffset, Utils.hbaseHosts, Utils.hbasePort)

    val kafkaStream = kafkaReader.getKafkaStream()

    val lines = kafkaStream.map(_._2).flatMap(str => {
      str.split("}}").map(_ + "}}")
    })

    val insertRecords = lines.filter(str => str.contains(Utils.insertOpt)).map(i => {
      Utils.insertRecordsConvert(i) match {
        case Some(s) => s
      }
    })

    insertRecords.foreachRDD(rdd => {

      HiveUtils.query_init(sc)
      val entrust_details = hvc.read.json(rdd)

      if (HiveUtils.schemaFieldsCheck(entrust_details.schema, "APP_TIMESTAMP", "APP_SNO", "CUST_CODE", "CUACCT_CODE",
        "INST_CODE", "INST_SNAME", "ORD_AMT", "ORD_QTY", "TRD_ID", "CANCEL_FLAG", "TRD_DATE")) {

        entrust_details.registerTempTable("entrust_details")

        hvc.udf.register("otcOrderTypeConvert", Utils.otcOrderTypeConvert)
        hvc.udf.register("otcTimestampConvert", Utils.otcTimestampConvert)
        hvc.udf.register("otcAmtConvert", Utils.otcAmtConvert)
        hvc.udf.register("otcCrtPositionStr", Utils.otcCrtPositionStr)

        val df = hvc.sql("select otcCrtPositionStr(e.app_timestamp, e.app_sno) as position_str, " +
          "e.cust_code as client_id, " +
          "e.cuacct_code as fund_account, " +
          "e.inst_code as fund_code, " +
          "e.inst_sname as fund_name, " +
          "otcAmtConvert(e.ord_amt, e.ord_qty) as amt, " +
          "otcOrderTypeConvert(e.trd_id) as ord_type, " +
          "otcTimestampConvert(e.app_timestamp) as ord_time, " +
          "e.trd_date as trd_date " +
          "from entrust_details e " +
          "where e.cancel_flag  = '0' and " +
          "e.trd_id in ('110','111','112') and " +
          "e.cust_code is not null and " +
          "e.cuacct_code is not null and " +
          "e.inst_code is not null and " +
          "e.inst_sname is not null and " +
          "e.ord_amt is not null and " +
          "e.ord_qty is not null and " +
          "e.trd_id is not null and " +
          "e.app_timestamp is not null and " +
          "e.trd_date is not null").repartition(2)

        df.foreachPartition(iter => {

          var jedisCluster: JedisCluster = null
          val hbaseConnect: Connection = HbaseUtils.getConnect()
          var table: Table = null

          try {
            jedisCluster = new JedisCluster(Utils.jedisClusterNodes, 2000, 100, Utils.jedisConf)
            val tableName = TableName.valueOf(Utils.hbaseTOtcorderDetails)
            table = hbaseConnect.getTable(tableName)

            for (r <- iter) {
              val key = String.format(Utils.redisClientRelKey, r(1).toString)
              val client = jedisCluster.hgetAll(key)

              if (!client.isEmpty) {
                //匹配客户-员工关系表中存在的记录
                val client_name = client.get("client_name")

                val mapper = new ObjectMapper()
                mapper.registerModule(DefaultScalaModule)
                val staff_list = mapper.readValue(client.get("staff_list"), classOf[util.ArrayList[immutable.Map[String, String]]])

                val position_str = r(0).toString
                val client_id = (math.BigInt(r(1).toString) + math.BigInt("100000000000000000000")).toString()
                val fund_account = r(2).toString
                val fund_code = r(3).toString
                val fund_name = r(4).toString
                val amt = r(5).toString
                val ord_type = r(6).toString
                val ord_time = r(7).toString
                val trd_date = Utils.initdateCvt(r(8).toString)

                if (trd_date >= Utils.getSpecDay(0, "yyyy-MM-dd")) {
                  for (i <- staff_list) {

                    val staff_id = i.getOrElse("id", "")
                    val staff_name = i.getOrElse("name", "")
                    val ts = (10000000000L - Utils.getUnixStamp(ord_time, "yyyy-MM-dd HH:mm:ss")).toString

                    //staff_id 逆序 同一员工下按position_str排序
                    val arr = Array(staff_id.reverse, ts, trd_date, position_str, fund_account)
                    val rowkey = arr.mkString(",")
                    val putTry = new Put(Bytes.toBytes(rowkey))
                    putTry.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("exist"), Bytes.toBytes("1"))

                    if (table.checkAndPut(Bytes.toBytes(rowkey), Bytes.toBytes("cf"), Bytes.toBytes("exist"), null, putTry)) {
                      //检验hbase无此明细 确保重提唯一性
                      //hbase 记录明细
                      val put = new Put(Bytes.toBytes(rowkey))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("position_str"), Bytes.toBytes(position_str))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("client_id"), Bytes.toBytes(client_id))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("fund_account"), Bytes.toBytes(fund_account))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("fund_code"), Bytes.toBytes(fund_code))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("fund_name"), Bytes.toBytes(fund_name))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("amt"), Bytes.toBytes(amt))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ord_type"), Bytes.toBytes(ord_type))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ord_time"), Bytes.toBytes(ord_time))

                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("client_name"), Bytes.toBytes(client_name))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("staff_id"), Bytes.toBytes(staff_id))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("staff_name"), Bytes.toBytes(staff_name))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("init_date"), Bytes.toBytes(trd_date))

                      table.put(put)

                      //当日聚合统计
                      if (trd_date == Utils.getSpecDay(0, "yyyy-MM-dd")) {
                        //记录条数汇总
                        jedisCluster.hincrBy(String.format(Utils.redisStaffInfoKey, staff_id), "otcorder_count", 1)
                      }
                    }
                  }
                }
              }
            }
          } catch {
            case ex: Exception => {
              ex.printStackTrace()
              logger.warn("redis/hbase error")
            }
          } finally {
            if (jedisCluster != null)
              jedisCluster.close()
            if (table != null)
              table.close()
          }
        })
      }
    })
  }

  def otcbookorderPrc(sc: SparkContext, ssc: StreamingContext, hvc: HiveContext): Unit = {
    val kafkaReader = new KafkaReader[String, String, StringDecoder, StringDecoder](ssc, Utils.brokerList, Utils.topicOggOtcbookorder,
                                                                                    Utils.hbaseTKafkaOffset, Utils.hbaseHosts, Utils.hbasePort)

    val kafkaStream = kafkaReader.getKafkaStream()

    val lines = kafkaStream.map(_._2).flatMap(str => {
      str.split("}}").map(_ + "}}")
    })

    val insertRecords = lines.filter(str => str.contains(Utils.insertOpt)).map(i => {
      Utils.insertRecordsConvert(i) match {
        case Some(s) => s
      }
    })

    insertRecords.foreachRDD(rdd => {

      HiveUtils.query_init(sc)
      val entrust_details = hvc.read.json(rdd)

      if (HiveUtils.schemaFieldsCheck(entrust_details.schema, "BOOK_TIMESTAMP", "BOOK_SNO", "CUST_CODE", "CUACCT_CODE",
        "INST_CODE", "INST_SNAME", "ORD_AMT", "ORD_QTY", "TRD_ID", "CANCEL_FLAG")) {

        entrust_details.registerTempTable("entrust_details")

        hvc.udf.register("otcOrderTypeConvert", Utils.otcOrderTypeConvert)
        hvc.udf.register("otcTimestampConvert", Utils.otcTimestampConvert)
        hvc.udf.register("otcAmtConvert", Utils.otcAmtConvert)
        hvc.udf.register("otcCrtPositionStr", Utils.otcCrtPositionStr)

        val df = hvc.sql("select otcCrtPositionStr(e.book_timestamp, e.book_sno) as position_str, " +
          "e.cust_code as client_id, " +
          "e.cuacct_code as fund_account, " +
          "e.inst_code as fund_code, " +
          "e.inst_sname as fund_name, " +
          "otcAmtConvert(e.ord_amt, e.ord_qty) as amt, " +
          "otcOrderTypeConvert(e.trd_id) as ord_type, " +
          "otcTimestampConvert(e.book_timestamp) as ord_time " +
          "from entrust_details e " +
          "where e.cancel_flag  = '0' and " +
          "e.trd_id in ('110','111','112') and " +
          "e.cust_code is not null and " +
          "e.cuacct_code is not null and " +
          "e.inst_code is not null and " +
          "e.inst_sname is not null and " +
          "e.ord_amt is not null and " +
          "e.ord_qty is not null and " +
          "e.trd_id is not null and " +
          "e.book_timestamp is not null").repartition(2)

        df.foreachPartition(iter => {

          var jedisCluster: JedisCluster = null
          val hbaseConnect: Connection = HbaseUtils.getConnect()
          var table: Table = null

          try {
            jedisCluster = new JedisCluster(Utils.jedisClusterNodes, 2000, 100, Utils.jedisConf)
            val tableName = TableName.valueOf(Utils.hbaseTOtcbookorderDetails)
            table = hbaseConnect.getTable(tableName)

            for (r <- iter) {
              val key = String.format(Utils.redisClientRelKey, r(1).toString)
              val client = jedisCluster.hgetAll(key)

              if (!client.isEmpty) {
                //匹配客户-员工关系表中存在的记录
                val client_name = client.get("client_name")

                val mapper = new ObjectMapper()
                mapper.registerModule(DefaultScalaModule)
                val staff_list = mapper.readValue(client.get("staff_list"), classOf[util.ArrayList[immutable.Map[String, String]]])

                val position_str = r(0).toString
                val client_id = (math.BigInt(r(1).toString) + math.BigInt("100000000000000000000")).toString()
                val fund_account = r(2).toString
                val fund_code = r(3).toString
                val fund_name = r(4).toString
                val amt = r(5).toString
                val ord_type = r(6).toString
                val ord_time = r(7).toString

                if (ord_time.split(" ")(0) >= Utils.getSpecDay(0, "yyyy-MM-dd")) {
                  for (i <- staff_list) {

                    val staff_id = i.getOrElse("id", "")
                    val staff_name = i.getOrElse("name", "")
                    val ts = (10000000000L - Utils.getUnixStamp(ord_time, "yyyy-MM-dd HH:mm:ss")).toString

                    //staff_id 逆序 同一员工下按position_str排序
                    val arr = Array(staff_id.reverse, ts, ord_time.split(" ")(0), position_str, fund_account)
                    val rowkey = arr.mkString(",")
                    val putTry = new Put(Bytes.toBytes(rowkey))
                    putTry.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("exist"), Bytes.toBytes("1"))

                    if (table.checkAndPut(Bytes.toBytes(rowkey), Bytes.toBytes("cf"), Bytes.toBytes("exist"), null, putTry)) {
                      //检验hbase无此明细 确保重提唯一性
                      //hbase 记录明细
                      val put = new Put(Bytes.toBytes(rowkey))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("position_str"), Bytes.toBytes(position_str))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("client_id"), Bytes.toBytes(client_id))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("fund_account"), Bytes.toBytes(fund_account))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("fund_code"), Bytes.toBytes(fund_code))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("fund_name"), Bytes.toBytes(fund_name))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("amt"), Bytes.toBytes(amt))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ord_type"), Bytes.toBytes(ord_type))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("ord_time"), Bytes.toBytes(ord_time))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("init_date"), Bytes.toBytes(ord_time.split(" ")(0)))

                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("client_name"), Bytes.toBytes(client_name))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("staff_id"), Bytes.toBytes(staff_id))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("staff_name"), Bytes.toBytes(staff_name))

                      table.put(put)

                      //当日聚合统计
                      if (ord_time.split(" ")(0) == Utils.getSpecDay(0, "yyyy-MM-dd")) {
                        //记录条数汇总
                        jedisCluster.hincrBy(String.format(Utils.redisStaffInfoKey, staff_id), "otcbookorder_count", 1)
                      }
                    }
                  }
                }
              }
            }
          } catch {
            case ex: Exception => {
              ex.printStackTrace()
              logger.warn("redis/hbase error")
            }
          } finally {
            if (jedisCluster != null)
              jedisCluster.close()
            if (table != null)
              table.close()
          }
        })
      }
    })
  }

  def cbsentrustPrc(sc: SparkContext, ssc: StreamingContext, hvc: HiveContext): Unit = {
    val kafkaReader = new KafkaReader[String, String, StringDecoder, StringDecoder](ssc, Utils.brokerList, Utils.topicOggCbsentrust,
                                                                                    Utils.hbaseTKafkaOffset, Utils.hbaseHosts, Utils.hbasePort)

    val kafkaStream = kafkaReader.getKafkaStream()

    val lines = kafkaStream.map(_._2).flatMap(str => {
      str.split("}}").map(_ + "}}")
    })
    lines.persist()

    val insertRecords = lines.filter(str => str.contains(Utils.insertOpt)).map(i => {
      Utils.insertRecordsConvert(i) match {
        case Some(s) => s
      }
    })

    val updateRecords = lines.filter(str => str.contains(Utils.updateOpt) && str.contains("BUSINESS_BALANCE")).map(u => {
      Utils.updateRecordsConvert(u) match {
        case Some(m) => m
      }
    })

    insertRecords.foreachRDD(rdd => {

      HiveUtils.readStkcodeFromHive(sc, hvc)
      HiveUtils.readSystemdictFromHive(sc, hvc)

      val entrust_details = hvc.read.json(rdd)

      if (HiveUtils.schemaFieldsCheck(entrust_details.schema, "POSITION_STR", "BRANCH_NO", "FUND_ACCOUNT", "CLIENT_ID",
        "CURR_DATE", "CURR_TIME", "STOCK_CODE", "ENTRUST_PRICE", "ENTRUST_AMOUNT", "EXCHANGE_TYPE",
        "OP_ENTRUST_WAY", "ENTRUST_BS", "STOCK_TYPE", "INIT_DATE")) {

        entrust_details.registerTempTable("entrust_details")

        hvc.sql("select * from tmp_sysdict WHERE dict_entry = 1201").registerTempTable("tmp_entrustway")
        hvc.sql("select * from tmp_sysdict WHERE dict_entry = 1204").registerTempTable("tmp_entrustbs")
        hvc.sql("select * from tmp_sysdict WHERE dict_entry = 1301").registerTempTable("tmp_exchangetype")
        hvc.sql("select * from tmp_sysdict WHERE dict_entry = 1101").registerTempTable("tmp_moneytype")
        hvc.udf.register("concatDateTime", Utils.concatDateTime2)

        val today = Utils.getSpecDay(0, "yyyyMMdd")
        val df = hvc.sql("select e.position_str, e.branch_no, e.fund_account, e.client_id, " +
          "concatDateTime(e.curr_date, e.curr_time) as curr_time, " +
          "e.stock_code as stkcode, COALESCE(c.stock_name,'') as stkname, " +
          "COALESCE(mt.DICT_PROMPT,'') as money_type_name, " +
          "COALESCE(eb.DICT_PROMPT,'') as remark, " +
          "e.entrust_price as entrust_price, " +
          "e.entrust_amount as entrust_amount, " +
          "round(e.entrust_price*e.entrust_amount,2) as entrust_balance, " +
          "COALESCE(ew.DICT_PROMPT,'') as op_entrust_way_name, " +
          "COALESCE(et.DICT_PROMPT,'') as market_name, " +
          "e.exchange_type as exchange_type, " +
          "e.stock_type, e.init_date " +
          "from entrust_details e " +
          "left outer join tmp_stkcode c " +
          "on e.exchange_type = c.exchange_type and e.stock_code = c.stock_code " +
          "left outer join tmp_entrustway ew " +
          "on e.op_entrust_way = ew.subentry " +
          "left outer join tmp_entrustbs eb " +
          "on e.entrust_bs = eb.subentry " +
          "left outer join tmp_exchangetype et " +
          "on e.exchange_type = et.subentry " +
          "left outer join tmp_moneytype mt " +
          "on c.money_type = mt.subentry " +
          s"where e.init_date >= '${today}' and " +
          "e.entrust_type = '0' and " +
          "e.position_str is not null and " +
          "e.branch_no is not null and " +
          "e.fund_account is not null and " +
          "e.client_id is not null and " +
          "e.curr_date is not null and " +
          "e.curr_time is not null and " +
          "e.stock_code is not null and " +
          "e.entrust_price is not null and " +
          "e.entrust_amount is not null and " +
          "e.init_date is not null").repartition(2)

        df.foreachPartition(iter => {

          val hbaseConnect: Connection = HbaseUtils.getConnect()
          var jedisCluster: JedisCluster = null
          var tableDetails: Table = null
          var tableMapping: Table = null

          try {
            jedisCluster = new JedisCluster(Utils.jedisClusterNodes, 2000, 100, Utils.jedisConf)
            var tableName = TableName.valueOf(Utils.hbaseTEntrustDetails)
            tableDetails = hbaseConnect.getTable(tableName)
            tableName = TableName.valueOf(Utils.hbaseTEntrustMapping)
            tableMapping = hbaseConnect.getTable(tableName)

            for (r <- iter) {
              val key = String.format(Utils.redisClientRelKey, r(3).toString)
              val client = jedisCluster.hgetAll(key)

              if (!client.isEmpty) {
                //匹配客户-员工关系表中存在的记录
                val client_name = client.get("client_name")

                val mapper = new ObjectMapper()
                mapper.registerModule(DefaultScalaModule)
                val staff_list = mapper.readValue(client.get("staff_list"), classOf[util.ArrayList[immutable.Map[String, String]]])

                val position_str = "cbs" + r(0).toString
                val branch_no = r(1).toString
                val fund_account = r(2).toString
                val client_id = (math.BigInt(r(3).toString) + math.BigInt("100000000000000000000")).toString()
                val curr_time = r(4).toString
                val stkcode = r(5).toString
                var stkname = r(6).toString
                var moneytype_name = r(7).toString
                val remark = r(8).toString
                val entrust_price = r(9).toString
                val entrust_amount = r(10).toString
                val entrust_balance = r(11).toString
                val op_entrust_way_name = r(12).toString
                val market_name = r(13).toString
                val exchange_type = r(14).toString
                val stock_type = r(15).toString
                val init_date = Utils.initdateCvt(r(16).toString)

                if (stkname.length == 0 || moneytype_name.length == 0) {
                  //通过hbase查询
                  val stockInfo = HbaseUtils.getStkcodeFromHbase(hbaseConnect, exchange_type, stkcode)

                  if (stkname.length == 0)
                    stkname = stockInfo._1
                  if (moneytype_name.length == 0)
                    moneytype_name = stockInfo._2
                }

                for (i <- staff_list) {

                  val staff_id = i.getOrElse("id", "")
                  val staff_name = i.getOrElse("name", "")
                  val ts = (10000000000L - Utils.getUnixStamp(curr_time, "yyyy-MM-dd HH:mm:ss")).toString

                  //staff_id 逆序 同一员工下按position_str排序
                  val arr = Array(staff_id.reverse, ts, init_date, position_str, client_name, fund_account, stkcode, stock_type)
                  val rowkey = arr.mkString(",")
                  val putTry = new Put(Bytes.toBytes(rowkey))
                  putTry.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("exist"), Bytes.toBytes("1"))

                  if (tableDetails.checkAndPut(Bytes.toBytes(rowkey), Bytes.toBytes("cf"), Bytes.toBytes("exist"), null, putTry)) {
                    //检验hbase无此明细 确保重提唯一性
                    //hbase 记录明细
                    val put = new Put(Bytes.toBytes(rowkey))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("position_str"), Bytes.toBytes(position_str))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("branch_no"), Bytes.toBytes(branch_no))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("fund_account"), Bytes.toBytes(fund_account))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("client_id"), Bytes.toBytes(client_id))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("curr_time"), Bytes.toBytes(curr_time))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("stkcode"), Bytes.toBytes(stkcode))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("stkname"), Bytes.toBytes(stkname))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("moneytype_name"), Bytes.toBytes(moneytype_name))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("remark"), Bytes.toBytes(remark))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("entrust_price"), Bytes.toBytes(entrust_price))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("entrust_amount"), Bytes.toBytes(entrust_amount))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("entrust_balance"), Bytes.toBytes(entrust_balance))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("op_entrust_way_name"), Bytes.toBytes(op_entrust_way_name))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("market_name"), Bytes.toBytes(market_name))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("stock_type"), Bytes.toBytes(stock_type))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("init_date"), Bytes.toBytes(init_date))

                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("client_name"), Bytes.toBytes(client_name))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("staff_id"), Bytes.toBytes(staff_id))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("staff_name"), Bytes.toBytes(staff_name))

                    tableDetails.put(put)

                    val putMapping = new Put(Bytes.toBytes(position_str.reverse))
                    putMapping.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(rowkey), Bytes.toBytes("1"))
                    putMapping.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("init_date"), Bytes.toBytes(init_date))
                    tableMapping.put(putMapping)

                    //记录条数汇总
                    jedisCluster.hincrBy(String.format(Utils.redisStaffInfoKey, staff_id), "cbsentrust_count", 1)

                    //实时汇总部分
                    val entrustKey = String.format(Utils.redisAggregateEntrustKey, init_date, staff_id)

                    if (!jedisCluster.hexists(entrustKey, "entrust_count")) {
                      jedisCluster.hincrBy(entrustKey, "entrust_count", 0)
                      jedisCluster.expireAt(entrustKey, Utils.getUnixStamp(Utils.dateStringAddDays(init_date, 1), "yyyy-MM-dd"))
                    }
                    jedisCluster.hincrBy(entrustKey, "entrust_count", 1)
                    jedisCluster.hincrByFloat(entrustKey, "entrust_balance", entrust_balance.toDouble)
                  }
                }
              }
            }
          } catch {
            case ex: Exception => {
              ex.printStackTrace()
              logger.warn("redis/hbase error")
            }
          } finally {
            if (jedisCluster != null)
              jedisCluster.close()
            if (tableDetails != null)
              tableDetails.close()
            if (tableMapping != null)
              tableMapping.close()
          }
        })
      }
    })

    updateRecords.foreachRDD(rdd => {

      val rdd0 = rdd.map(m => (m("POSITION_STR")._1, m)).partitionBy(new org.apache.spark.HashPartitioner(2))
      //过滤出 更新BUSINESS_BALANCE的记录
      val rdd1 = rdd0.filter(m => (m._2.contains("BUSINESS_BALANCE") &&
        (m._2("BUSINESS_BALANCE")._1 != m._2("BUSINESS_BALANCE")._2)))
      //相同postion_str 到一个分区 对应操作按pos排序
      val rdd2 = rdd1.groupByKey().map(x => {
        val list = x._2.toList.sortWith((m1, m2) => {
          m1("pos")._1 < m2("pos")._1
        })
        (x._1, list(list.length-1)("BUSINESS_BALANCE")._2)
      })

      rdd2.foreachPartition(iter => {

        val hbaseConnect: Connection = HbaseUtils.getConnect()
        var tableMapping: Table = null
        var tableDetails: Table = null
        var jedisCluster: JedisCluster = null

        try {
          var tableName = TableName.valueOf(Utils.hbaseTEntrustMapping)
          tableMapping = hbaseConnect.getTable(tableName)
          tableName = TableName.valueOf(Utils.hbaseTEntrustDetails)
          tableDetails = hbaseConnect.getTable(tableName)
          jedisCluster = new JedisCluster(Utils.jedisClusterNodes, 2000, 100, Utils.jedisConf)

          for ((s, bal) <- iter) {

            val rowkey = ("cbs" + s).reverse
            val get = new Get(Bytes.toBytes(rowkey))
            val rst = tableMapping.get(get)

            if (!rst.isEmpty) {
              //对应关系
              val columns = rst.rawCells().map(c => Bytes.toString(c.getQualifierArray,
                c.getQualifierOffset,
                c.getQualifierLength)).filter(s => {s != "init_date"})
              columns.map(t => {
                //员工明细表
                val key = t
                val get = new Get(Bytes.toBytes(key))
                get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("entrust_balance"))
                val result = tableDetails.get(get)
                if (!result.isEmpty) {
                  //读取更新前entrust_balance
                  val preBal = Bytes.toString(result.getValue(Bytes.toBytes("cf"), Bytes.toBytes("entrust_balance"))).toDouble
                  //                  println(s"${key}: ${preBal}->${bal}")

                  val put = new Put(Bytes.toBytes(key))
                  //明细更新为最新值
                  put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("entrust_balance"), Bytes.toBytes(bal))
                  put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("last_update"), Bytes.toBytes(Utils.getSpecDay(0, "yyyy-MM-dd HH:mm:ss")))
                  tableDetails.put(put)

                  //redis金额统计更新
                  //实时汇总部分
                  val init_date = key.split(",")(2)
                  val delta = bal.toDouble - preBal
                  val staff_id = key.split(",")(0).reverse
                  val entrustKey = String.format(Utils.redisAggregateEntrustKey, init_date, staff_id)

                  if (!jedisCluster.hexists(entrustKey, "entrust_count")) {
                    jedisCluster.hincrBy(entrustKey, "entrust_count", 0)
                    jedisCluster.expireAt(entrustKey, Utils.getUnixStamp(Utils.dateStringAddDays(init_date, 1), "yyyy-MM-dd"))
                  }
                  jedisCluster.hincrByFloat(entrustKey, "entrust_balance", delta)
                }
              })
            }
          }
        } catch {
          case ex: Exception => {
            ex.printStackTrace()
          }
        } finally {
          if (tableMapping != null)
            tableMapping.close()
          if (tableDetails != null)
            tableDetails.close()
          if (jedisCluster != null)
            jedisCluster.close()
        }
      })
    })
  }

  def cbsrealtimePrc(sc: SparkContext, ssc: StreamingContext, hvc: HiveContext): Unit = {
    val kafkaReader = new KafkaReader[String, String, StringDecoder, StringDecoder](ssc, Utils.brokerList, Utils.topicOggCbsrealtime,
                                                                                    Utils.hbaseTKafkaOffset, Utils.hbaseHosts, Utils.hbasePort)

    val kafkaStream = kafkaReader.getKafkaStream()

    val lines = kafkaStream.map(_._2).flatMap(str => {
      str.split("}}").map(_ + "}}")
    })

    val insertRecords = lines.filter(str => str.contains(Utils.insertOpt)).map(i => {
      Utils.insertRecordsConvert(i) match {
        case Some(s) => s
      }
    })

    insertRecords.foreachRDD(rdd => {

      HiveUtils.readStkcodeFromHive(sc, hvc)
      HiveUtils.readSystemdictFromHive(sc, hvc)
      HiveUtils.readBranchFromHive(sc, hvc)

      val realtime_details = hvc.read.json(rdd)

      if (HiveUtils.schemaFieldsCheck(realtime_details.schema, "POSITION_STR", "FUND_ACCOUNT", "CLIENT_ID",
        "CURR_DATE", "CURR_TIME", "STOCK_CODE", "BUSINESS_PRICE", "BUSINESS_AMOUNT",
        "BUSINESS_BALANCE", "EXCHANGE_TYPE", "ENTRUST_BS", "REAL_TYPE", "BRANCH_NO",
        "REAL_STATUS", "INIT_DATE")) {

        realtime_details.registerTempTable("realtime_details")

        hvc.sql("select * from tmp_sysdict WHERE dict_entry = 1204").registerTempTable("tmp_entrustbs")
        hvc.sql("select * from tmp_sysdict WHERE dict_entry = 1301").registerTempTable("tmp_exchangetype")
        hvc.sql("select * from tmp_sysdict WHERE dict_entry = 1212").registerTempTable("tmp_realtype")
        hvc.sql("select * from tmp_sysdict WHERE dict_entry = 1101").registerTempTable("tmp_moneytype")
        hvc.udf.register("concatDateTime", Utils.concatDateTime)

        val today = Utils.getSpecDay(0, "yyyyMMdd")
        val df = hvc.sql("select r.position_str, r.fund_account, r.client_id, " +
          "concatDateTime(r.curr_date, r.curr_time) as curr_time, " +
          "r.stock_code as stkcode, COALESCE(c.stock_name,'') as stkname, " +
          "COALESCE(mt.DICT_PROMPT,'') as money_type_name, " +
          "COALESCE(eb.DICT_PROMPT,'') as remark, " +
          "r.business_price as price, " +
          "r.business_amount as amount, " +
          "r.business_balance as balance, " +
          "COALESCE(et.DICT_PROMPT,'') as market_name, " +
          "COALESCE(rt.DICT_PROMPT,'') as real_name, " +
          "r.branch_no as branch_no, " +
          "COALESCE(br.branch_name,'') as branch_name, " +
          "r.real_status as real_status, " +
          "r.real_type as real_type, " +
          "r.exchange_type as exchange_type, " +
          "COALESCE(c.stock_type,'') as stock_type, " +
          "r.init_date as init_date " +
          "from realtime_details r " +
          "left outer join tmp_stkcode c " +
          "on r.exchange_type = c.exchange_type and r.stock_code = c.stock_code " +
          "left outer join tmp_entrustbs as eb " +
          "on r.entrust_bs = eb.subentry " +
          "left outer join tmp_realtype as rt " +
          "on r.real_type = rt.subentry " +
          "left outer join tmp_exchangetype as et " +
          "on r.exchange_type = et.subentry " +
          "left outer join tmp_moneytype as mt " +
          "on c.money_type = mt.subentry " +
          "left outer join tmp_allbranch as br " +
          "on r.branch_no = br.branch_no " +
          s"where r.init_date >= '${today}' and " +
          "r.real_status != '2' and " +
          "r.real_type != '2' and " +
          "r.position_str is not null and " +
          "r.fund_account is not null and " +
          "r.client_id is not null and " +
          "r.curr_date is not null and " +
          "r.curr_time is not null and " +
          "r.stock_code is not null and " +
          "r.business_price is not null and " +
          "r.business_amount is not null and " +
          "r.business_balance is not null and " +
          "r.branch_no is not null and " +
          "r.real_status is not null and " +
          "r.real_type is not null and " +
          "r.init_date is not null").repartition(2)

        df.foreachPartition(iter => {

          val hbaseConnect: Connection = HbaseUtils.getConnect()
          var jedisCluster: JedisCluster = null
          var table: Table = null

          try {
            jedisCluster = new JedisCluster(Utils.jedisClusterNodes, 2000, 100, Utils.jedisConf)
            val tableName = TableName.valueOf(Utils.hbaseTRealtimeDetails)
            table = hbaseConnect.getTable(tableName)

            for (r <- iter) {
              val key = String.format(Utils.redisClientRelKey, r(2).toString)
              val client = jedisCluster.hgetAll(key)

              if (!client.isEmpty) {
                //匹配客户-员工关系表中存在的记录
                val client_name = client.get("client_name")

                val mapper = new ObjectMapper()
                mapper.registerModule(DefaultScalaModule)
                val staff_list = mapper.readValue(client.get("staff_list"), classOf[util.ArrayList[immutable.Map[String, String]]])

                val position_str = "cbs" + r(0).toString
                val fund_account = r(1).toString
                val client_id = (math.BigInt(r(2).toString) + math.BigInt("100000000000000000000")).toString()
                val curr_time = r(3).toString
                val stkcode = r(4).toString
                var stkname = r(5).toString
                var moneytype_name = r(6).toString
                val remark = r(7).toString
                val price = r(8).toString
                val amount = r(9).toString
                val balance = r(10).toString
                val market_name = r(11).toString
                val real_name = r(12).toString
                val branch_no = r(13).toString
                val branch_name = r(14).toString
                val real_status = r(15).toString
                val real_type = r(16).toString
                val exchange_type = r(17).toString
                var stock_type = r(18).toString
                val init_date = Utils.initdateCvt(r(19).toString)

                if (stkname.length == 0 || moneytype_name.length == 0 || stock_type.length == 0) {
                  //通过hbase查询
                  val stockInfo = HbaseUtils.getStkcodeFromHbase(hbaseConnect, exchange_type, stkcode)

                  if (stkname.length == 0)
                    stkname = stockInfo._1
                  if (moneytype_name.length == 0)
                    moneytype_name = stockInfo._2
                  if (stock_type.length == 0)
                    stock_type = stockInfo._3
                }

                if (stock_type != "4") {
                  //非普通申购
                  for (i <- staff_list) {
                    val staff_id = i.getOrElse("id", "")
                    val staff_name = i.getOrElse("name", "")
                    val ts = (10000000000L - Utils.getUnixStamp(curr_time, "yyyy-MM-dd HH:mm:ss")).toString

                    //staff_id 逆序 同一员工下按position_str排序
                    val arr = Array(staff_id.reverse, ts, init_date, position_str, client_name, fund_account, stkcode, real_name)
                    val rowkey = arr.mkString(",")
                    val putTry = new Put(Bytes.toBytes(rowkey))
                    putTry.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("exist"), Bytes.toBytes("1"))

                    if (table.checkAndPut(Bytes.toBytes(rowkey), Bytes.toBytes("cf"), Bytes.toBytes("exist"), null, putTry)) {
                      //检验hbase无此明细 确保重提唯一性
                      //hbase 记录明细
                      val put = new Put(Bytes.toBytes(rowkey))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("position_str"), Bytes.toBytes(position_str))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("fund_account"), Bytes.toBytes(fund_account))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("client_id"), Bytes.toBytes(client_id))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("curr_time"), Bytes.toBytes(curr_time))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("stkcode"), Bytes.toBytes(stkcode))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("stkname"), Bytes.toBytes(stkname))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("moneytype_name"), Bytes.toBytes(moneytype_name))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("remark"), Bytes.toBytes(remark))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("price"), Bytes.toBytes(price))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("amount"), Bytes.toBytes(amount))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("balance"), Bytes.toBytes(balance))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("market_name"), Bytes.toBytes(market_name))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("real_name"), Bytes.toBytes(real_name))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("branch_no"), Bytes.toBytes(branch_no))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("branch_name"), Bytes.toBytes(branch_name))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("real_status"), Bytes.toBytes(real_status))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("real_type"), Bytes.toBytes(real_type))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("init_date"), Bytes.toBytes(init_date))

                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("client_name"), Bytes.toBytes(client_name))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("staff_id"), Bytes.toBytes(staff_id))
                      put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("staff_name"), Bytes.toBytes(staff_name))

                      table.put(put)

                      //当日聚合统计
                      if (init_date == Utils.getSpecDay(0, "yyyy-MM-dd")) {
                        //记录条数汇总
                        jedisCluster.hincrBy(String.format(Utils.redisStaffInfoKey, staff_id), "cbsrealtime_count", 1)

                        //实时汇总部分
                        val realtimeKey = String.format(Utils.redisAggregateRealtimeKey, staff_id)

                        if (!jedisCluster.hexists(realtimeKey, "deal_count")) {
                          jedisCluster.hincrBy(realtimeKey, "deal_count", 0)
                          jedisCluster.expireAt(realtimeKey, Utils.getUnixStamp(Utils.getSpecDay(1, "yyyy-MM-dd"), "yyyy-MM-dd"))
                        }
                        jedisCluster.hincrBy(realtimeKey, "deal_count", 1)
                        jedisCluster.hincrByFloat(realtimeKey, "deal_balance", balance.toDouble)

                        //员工下挂客户最大成交量
                        if (real_type == "0" && real_status  == "0") {
                          //买卖 已成交
                          val topdealKey = String.format(Utils.redisAggregateTopdealKey, staff_id)
                          val member = String.format("bno:%s:bname:%s:fund:%s:cn:%s:mt:%s:cid:%s",
                            branch_no, branch_name, fund_account, client_name, moneytype_name, client_id)

                          if (jedisCluster.zcard(topdealKey) == 0) {
                            jedisCluster.zincrby(topdealKey, 0.00, member)
                            jedisCluster.expireAt(topdealKey, Utils.getUnixStamp(Utils.getSpecDay(1, "yyyy-MM-dd"), "yyyy-MM-dd"))
                          }
                          jedisCluster.zincrby(topdealKey, balance.toDouble, member)
                        }
                      }
                    }
                  }
                }
              }
            }
          } catch {
            case ex: Exception => {
              ex.printStackTrace()
              logger.warn("redis/hbase error")
            }
          } finally {
            if (jedisCluster != null)
              jedisCluster.close()
            if (table != null)
              table.close()
          }
        })
      }
    })
  }
}
