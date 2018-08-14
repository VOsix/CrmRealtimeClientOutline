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
object OptCbpStkjourCtstOfentrustDetails {

  val conf = new SparkConf().setAppName("crmClientOutline_optcbpstkctstofDetails")
  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(20))
    val hvc = new HiveContext(sc)

    sc.setLogLevel("WARN")

    optentrustPrc(sc, ssc, hvc)
    optrealtimePrc(sc,ssc, hvc)
    cbpentrustPrc(sc, ssc, hvc)
    stockjourPrc(sc, ssc, hvc)
    ctstentrustPrc(sc, ssc, hvc)
    ofentrustPrc(sc, ssc, hvc)

    ssc.start()
    ssc.awaitTermination()
  }

  def optentrustPrc(sc: SparkContext, ssc: StreamingContext, hvc: HiveContext): Unit = {
    val kafkaReader = new KafkaReader[String, String, StringDecoder, StringDecoder](ssc, Utils.brokerList, Utils.topicOggOptentrust,
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

      HiveUtils.readOptcodeFromHive(sc, hvc)
      HiveUtils.readSystemdictFromHive(sc, hvc)

      val entrust_details = hvc.read.json(rdd)

      if (HiveUtils.schemaFieldsCheck(entrust_details.schema, "POSITION_STR", "BRANCH_NO", "FUND_ACCOUNT", "CLIENT_ID",
        "CURR_DATE", "CURR_TIME", "OPTION_CODE", "STOCK_CODE", "OPT_ENTRUST_PRICE", "ENTRUST_AMOUNT", "EXCHANGE_TYPE",
        "OP_ENTRUST_WAY", "ENTRUST_BS", "MONEY_TYPE", "INIT_DATE")) {

        entrust_details.registerTempTable("entrust_details")

        hvc.sql("select * from tmp_sysdict WHERE dict_entry = 1201").registerTempTable("tmp_entrustway")
        hvc.sql("select * from tmp_sysdict WHERE dict_entry = 36003").registerTempTable("tmp_remark")
//        hvc.sql("select * from tmp_sysdict WHERE dict_entry = 1204").registerTempTable("tmp_entrustbs")
        hvc.sql("select * from tmp_sysdict WHERE dict_entry = 1301").registerTempTable("tmp_exchangetype")
        hvc.sql("select * from tmp_sysdict WHERE dict_entry = 1101").registerTempTable("tmp_moneytype")
        hvc.udf.register("concatDateTime", Utils.concatDateTime)

        val today = Utils.getSpecDay(0, "yyyyMMdd")
        val df = hvc.sql("select e.position_str, e.branch_no, e.fund_account, e.client_id, " +
          "concatDateTime(e.curr_date, e.curr_time) as curr_time, " +
          "e.option_code as stkcode, COALESCE(c.option_name,'') as stkname, " +
          "COALESCE(mt.DICT_PROMPT,'') as money_type_name, " +
          "COALESCE(rm.DICT_PROMPT,'') as remark, " +
          "e.opt_entrust_price as entrust_price, " +
          "e.entrust_amount as entrust_amount, " +
          "round(e.opt_entrust_price*e.entrust_amount*COALESCE(c.amount_per_hand,10000),2) as entrust_balance, " +
          "COALESCE(ew.DICT_PROMPT,'') as op_entrust_way_name, " +
          "COALESCE(et.DICT_PROMPT,'') as market_name, " +
          "e.exchange_type as exchange_type, " +
          "e.option_code as option_code, " +
          "e.init_date as init_date " +
          "from entrust_details e " +
          "left outer join tmp_optcode c " +
          "on e.exchange_type = c.exchange_type and e.option_code = c.option_code " +
          "left outer join tmp_entrustway ew " +
          "on e.op_entrust_way = ew.subentry " +
//          "left outer join tmp_entrustbs eb " +
//          "on e.entrust_bs = eb.subentry " +
          "left outer join tmp_exchangetype et " +
          "on e.exchange_type = et.subentry " +
          "left outer join tmp_moneytype mt " +
          "on e.money_type = mt.subentry " +
          "left outer join tmp_remark rm " +
          "on e.entrust_oc = rm.subentry " +
          s"where e.init_date >= '${today}' and " +
          "e.entrust_type = '0' and " +
          "e.position_str is not null and " +
          "e.branch_no is not null and " +
          "e.fund_account is not null and " +
          "e.client_id is not null and " +
          "e.curr_date is not null and " +
          "e.curr_time is not null and " +
          "e.stock_code is not null and " +
          "e.option_code is not null and " +
          "e.opt_entrust_price is not null and " +
          "e.entrust_amount is not null and " +
          "e.init_date is not null").repartition(2)

        df.foreachPartition(iter => {

          var jedisCluster: JedisCluster = null
          val hbaseConnect: Connection = HbaseUtils.getConnect()
          var table: Table = null

          try {
            jedisCluster = new JedisCluster(Utils.jedisClusterNodes, 2000, 100, Utils.jedisConf)
            val tableName = TableName.valueOf(Utils.hbaseTOptentrustDetails)
            table = hbaseConnect.getTable(tableName)

            for (r <- iter) {
              val key = String.format(Utils.redisClientRelKey, r(3).toString)
              val client = jedisCluster.hgetAll(key)

              if (!client.isEmpty) {
                //匹配客户-员工关系表中存在的记录
                val client_name = client.get("client_name")

                val mapper = new ObjectMapper()
                mapper.registerModule(DefaultScalaModule)
                val staff_list = mapper.readValue(client.get("staff_list"), classOf[util.ArrayList[immutable.Map[String, String]]])

                val position_str = r(0).toString
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
                val option_code = r(15).toString
                val init_date = Utils.initdateCvt(r(16).toString)

                if (stkname.length == 0 || moneytype_name.length == 0) {
                  //通过hbase查询
                  val stockInfo = HbaseUtils.getOptcodeFromHbase(hbaseConnect, exchange_type, option_code)

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
                  val arr = Array(staff_id.reverse, ts, init_date, position_str, client_name, fund_account, stkcode)
                  val rowkey = arr.mkString(",")
                  val putTry = new Put(Bytes.toBytes(rowkey))
                  putTry.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("exist"), Bytes.toBytes("1"))

                  if (table.checkAndPut(Bytes.toBytes(rowkey), Bytes.toBytes("cf"), Bytes.toBytes("exist"), null, putTry)) {
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
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("init_date"), Bytes.toBytes(init_date))

                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("client_name"), Bytes.toBytes(client_name))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("staff_id"), Bytes.toBytes(staff_id))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("staff_name"), Bytes.toBytes(staff_name))

                    table.put(put)

                    //当日聚合统计
                    if (init_date == Utils.getSpecDay(0, "yyyy-MM-dd")) {
                      //记录条数汇总
                      jedisCluster.hincrBy(String.format(Utils.redisStaffInfoKey, staff_id), "optentrust_count", 1)
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

  def optrealtimePrc(sc: SparkContext, ssc: StreamingContext, hvc: HiveContext): Unit = {
    val kafkaReader = new KafkaReader[String, String, StringDecoder, StringDecoder](ssc, Utils.brokerList, Utils.topicOggOptrealtime,
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

      HiveUtils.readOptcodeFromHive(sc, hvc)
      HiveUtils.readSystemdictFromHive(sc, hvc)

      val optrealtime_details = hvc.read.json(rdd)

      if (HiveUtils.schemaFieldsCheck(optrealtime_details.schema, "POSITION_STR", "FUND_ACCOUNT", "CLIENT_ID",
        "CURR_DATE", "CURR_TIME", "STOCK_CODE", "OPTION_CODE", "OPT_BUSINESS_PRICE", "BUSINESS_AMOUNT",
        "BUSINESS_BALANCE", "EXCHANGE_TYPE", "ENTRUST_BS", "REAL_TYPE", "REAL_STATUS", "INIT_DATE")) {

        optrealtime_details.registerTempTable("realtime_details")

        hvc.sql("select * from tmp_sysdict WHERE dict_entry = 36003").registerTempTable("tmp_remark")
//        hvc.sql("select * from tmp_sysdict WHERE dict_entry = 1204").registerTempTable("tmp_entrustbs")
        hvc.sql("select * from tmp_sysdict WHERE dict_entry = 1301").registerTempTable("tmp_exchangetype")
        hvc.sql("select * from tmp_sysdict WHERE dict_entry = 1212").registerTempTable("tmp_realtype")
        hvc.sql("select * from tmp_sysdict WHERE dict_entry = 1101").registerTempTable("tmp_moneytype")
        hvc.udf.register("concatDateTime", Utils.concatDateTime)

        val today = Utils.getSpecDay(0, "yyyyMMdd")
        val df = hvc.sql("select r.position_str, r.fund_account, r.client_id, " +
          "concatDateTime(r.curr_date, r.curr_time) as curr_time, " +
          "r.option_code as stkcode, COALESCE(c.option_name,'') as stkname, " +
          "COALESCE(mt.DICT_PROMPT,'') as money_type_name, " +
          "COALESCE(rm.DICT_PROMPT,'') as remark, " +
          "r.opt_business_price as price, " +
          "r.business_amount as amount, " +
          "r.business_balance as balance, " +
          "COALESCE(et.DICT_PROMPT,'') as market_name, " +
          "COALESCE(rt.DICT_PROMPT,'') as real_name, " +
          "r.exchange_type as exchange_type, " +
          "r.option_code as option_code, " +
          "r.init_date as init_date " +
          "from realtime_details r " +
          "left outer join tmp_optcode c " +
          "on r.exchange_type = c.exchange_type and r.option_code = c.option_code " +
//          "left outer join tmp_entrustbs as eb " +
//          "on r.entrust_bs = eb.subentry " +
          "left outer join tmp_realtype as rt " +
          "on r.real_type = rt.subentry " +
          "left outer join tmp_exchangetype as et " +
          "on r.exchange_type = et.subentry " +
          "left outer join tmp_moneytype as mt " +
          "on c.money_type = mt.subentry " +
          "left outer join tmp_remark rm " +
          "on r.entrust_oc = rm.subentry " +
          s"where r.init_date >= '${today}' and " +
          "r.real_status != '2' and " +
          "r.position_str is not null and " +
          "r.fund_account is not null and " +
          "r.client_id is not null and " +
          "r.curr_date is not null and " +
          "r.curr_time is not null and " +
          "r.stock_code is not null and " +
          "r.option_code is not null and " +
          "r.opt_business_price is not null and " +
          "r.business_amount is not null and " +
          "r.business_balance is not null and " +
          "r.real_status is not null and " +
          "r.real_type is not null and " +
          "r.init_date is not null").repartition(2)
        //        df.show(10)

        df.foreachPartition(iter => {

          var jedisCluster: JedisCluster = null
          val hbaseConnect: Connection = HbaseUtils.getConnect()
          var table: Table = null

          try {
            jedisCluster = new JedisCluster(Utils.jedisClusterNodes, 2000, 100, Utils.jedisConf)
            val tableName = TableName.valueOf(Utils.hbaseTOptrealtimeDetails)
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

                val position_str = r(0).toString
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
                val exchange_type = r(13).toString
                val option_code = r(14).toString
                val init_date = Utils.initdateCvt(r(15).toString)

                if (stkname.length == 0 || moneytype_name.length == 0) {
                  //通过hbase查询
                  val stockInfo = HbaseUtils.getOptcodeFromHbase(hbaseConnect, exchange_type, option_code)

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
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("init_date"), Bytes.toBytes(init_date))

                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("client_name"), Bytes.toBytes(client_name))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("staff_id"), Bytes.toBytes(staff_id))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("staff_name"), Bytes.toBytes(staff_name))

                    table.put(put)

                    //当日聚合统计
                    if (init_date == Utils.getSpecDay(0, "yyyy-MM-dd")) {
                      //记录条数汇总
                      jedisCluster.hincrBy(String.format(Utils.redisStaffInfoKey, staff_id), "optreal_count", 1)
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

  def cbpentrustPrc(sc: SparkContext, ssc: StreamingContext, hvc: HiveContext): Unit = {
    val kafkaReader = new KafkaReader[String, String, StringDecoder, StringDecoder](ssc, Utils.brokerList, Utils.topicOggCbpentrust,
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

                val position_str = "cbp" + r(0).toString
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

                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("client_name"), Bytes.toBytes(client_name))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("staff_id"), Bytes.toBytes(staff_id))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("staff_name"), Bytes.toBytes(staff_name))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("init_date"), Bytes.toBytes(init_date))

                    tableDetails.put(put)

                    val putMapping = new Put(Bytes.toBytes(position_str.reverse))
                    putMapping.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(rowkey), Bytes.toBytes("1"))
                    putMapping.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("init_date"), Bytes.toBytes(init_date))
                    tableMapping.put(putMapping)

                    //记录条数汇总
                    jedisCluster.hincrBy(String.format(Utils.redisStaffInfoKey, staff_id), "cbpentrust_count", 1)

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
            //            if (hbaseConnect != null)
            //              hbaseConnect.close()
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

            val rowkey = ("cbp" + s).reverse
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

  def stockjourPrc(sc: SparkContext, ssc: StreamingContext, hvc: HiveContext): Unit = {
    val kafkaReader = new KafkaReader[String, String, StringDecoder, StringDecoder](ssc, Utils.brokerList, Utils.topicOggStockjour,
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

      HiveUtils.readBusinflagFromHive(sc, hvc)
      HiveUtils.readBranchFromHive(sc, hvc)
      HiveUtils.readSystemdictFromHive(sc, hvc)

      val stockjour_details = hvc.read.json(rdd)

      if (HiveUtils.schemaFieldsCheck(stockjour_details.schema, "POSITION_STR", "BRANCH_NO", "FUND_ACCOUNT", "CLIENT_ID",
        "CURR_DATE", "CURR_TIME", "OCCUR_AMOUNT", "BUSINESS_FLAG", "MONEY_TYPE", "STOCK_TYPE",
        "INIT_DATE")) {

        stockjour_details.registerTempTable("stockjour_details")

        hvc.sql("select * from tmp_sysdict WHERE dict_entry = 1101").registerTempTable("tmp_moneytype")
        hvc.udf.register("concatDateTime", Utils.concatDateTime2)

        val today = Utils.getSpecDay(0, "yyyyMMdd")
        val df = hvc.sql("select s.position_str, s.branch_no, s.fund_account, s.client_id, " +
          "concatDateTime(s.curr_date, s.curr_time) as curr_time, " +
          "COALESCE(br.branch_name,'') as branch_name, " +
          "COALESCE(mt.DICT_PROMPT,'') as money_type_name, " +
          "s.exchange_type, " +
          "s.stock_code, " +
          "s.occur_amount as occur_amount, " +
          "bf.business_name as remark, " +
          "s.init_date as init_date " +
          "from stockjour_details s " +
          "join tmp_allbranch br " +
          "on s.branch_no = br.branch_no " +
          "join tmp_businflag bf " +
          "on s.business_flag = bf.business_flag " +
          "join tmp_moneytype mt " +
          "on s.money_type = mt.subentry " +
          s"where s.init_date >= '${today}' and " +
          "s.stock_type not in ('4','7') and " +
          "s.business_flag in (3002,4008,4010) and " +
          "s.position_str is not null and " +
          "s.fund_account is not null and " +
          "s.client_id is not null and " +
          "s.curr_date is not null and " +
          "s.curr_time is not null and " +
          "s.branch_no is not null and " +
          "s.occur_amount is not null and " +
          "s.money_type is not null and " +
          "s.init_date is not null").repartition(2)
        //        df.show(10)

        df.foreachPartition(iter => {

          var jedisCluster: JedisCluster = null
          val hbaseConnect: Connection = HbaseUtils.getConnect()
          var tableDetails: Table = null
          var tablePrice: Table = null

          try {
            jedisCluster = new JedisCluster(Utils.jedisClusterNodes, 2000, 100, Utils.jedisConf)
            var tableName = TableName.valueOf(Utils.hbaseTStockjourDetails)
            tableDetails = hbaseConnect.getTable(tableName)
            tableName = TableName.valueOf(Utils.hbaseTPrice)
            tablePrice = hbaseConnect.getTable(tableName)

            for (r <- iter) {
              val key = String.format(Utils.redisClientRelKey, r(3).toString)
              val client = jedisCluster.hgetAll(key)

              if (!client.isEmpty) {
                //匹配客户-员工关系表中存在的记录
                val client_name = client.get("client_name")

                val mapper = new ObjectMapper()
                mapper.registerModule(DefaultScalaModule)
                val staff_list = mapper.readValue(client.get("staff_list"), classOf[util.ArrayList[immutable.Map[String, String]]])

                val position_str = r(0).toString
                val branch_no = r(1).toString
                val fund_account = r(2).toString
                val client_id = (math.BigInt(r(3).toString) + math.BigInt("100000000000000000000")).toString()
                val curr_time = r(4).toString
                val branch_name = r(5).toString
                val moneytype_name = r(6).toString
                val exchange_type = r(7).toString
                val stock_code = r(8).toString
                val occur_amount = r(9).toString
                val remark = r(10).toString
                val init_date = Utils.initdateCvt(r(11).toString)

                val keyPrice = s"${exchange_type}|${stock_code}"
                val get = new Get(Bytes.toBytes(keyPrice))
                get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("LAST_PRICE"))
                val lastPrice = Bytes.toString(tablePrice.get(get).getValue(Bytes.toBytes("cf"), Bytes.toBytes("LAST_PRICE")))
                val out_asset = f"${lastPrice.toDouble * occur_amount.toDouble}%.2f"

                for (i <- staff_list) {

                  val staff_id = i.getOrElse("id", "")
                  val staff_name = i.getOrElse("name", "")
                  val ts = (10000000000L - Utils.getUnixStamp(curr_time, "yyyy-MM-dd HH:mm:ss")).toString

                  //staff_id 逆序 同一员工下按position_str排序
                  val arr = Array(staff_id.reverse, ts, init_date, position_str)
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
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("branch_name"), Bytes.toBytes(branch_name))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("moneytype_name"), Bytes.toBytes(moneytype_name))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("exchange_type"), Bytes.toBytes(exchange_type))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("stock_code"), Bytes.toBytes(stock_code))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("occur_amount"), Bytes.toBytes(occur_amount))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("lastPrice"), Bytes.toBytes(lastPrice))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("out_asset"), Bytes.toBytes(out_asset))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("remark"), Bytes.toBytes(remark))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("init_date"), Bytes.toBytes(init_date))

                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("client_name"), Bytes.toBytes(client_name))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("staff_id"), Bytes.toBytes(staff_id))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("staff_name"), Bytes.toBytes(staff_name))

                    tableDetails.put(put)

                    //当日聚合统计
                    if (init_date == Utils.getSpecDay(0, "yyyy-MM-dd")) {
                      //记录条数汇总
                      jedisCluster.hincrBy(String.format(Utils.redisStaffInfoKey, staff_id), "stockjour_count", 1)

                      //实时汇总部分
                      val stockjourKey = String.format(Utils.redisAggregateStockjourKey, staff_id)
                      val member = String.format("bno:%s:bname:%s:fund:%s:cn:%s:mt:%s:remark:%s:cid:%s",
                        branch_no, branch_name, fund_account, client_name, moneytype_name, remark, client_id)

                      if (jedisCluster.zcard(stockjourKey) == 0) {
                        jedisCluster.zincrby(stockjourKey, 0.00, member)
                        jedisCluster.expireAt(stockjourKey, Utils.getUnixStamp(Utils.getSpecDay(1, "yyyy-MM-dd"), "yyyy-MM-dd"))
                      }
                      jedisCluster.zincrby(stockjourKey, out_asset.toDouble, member)
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
            if (tableDetails != null)
              tableDetails.close()
            if (tablePrice != null)
              tablePrice.close()
          }
        })
      }
    })
  }

  def ofentrustPrc(sc: SparkContext, ssc: StreamingContext, hvc: HiveContext): Unit = {
    val kafkaReader = new KafkaReader[String, String, StringDecoder, StringDecoder](ssc, Utils.brokerList, Utils.topicOggSecumentrust,
                                                                                    Utils.hbaseTKafkaOffset, Utils.hbaseHosts, Utils.hbasePort)

    //20180811 由集中交易场外产品委托明细由ofentrust改为secumentrust
    val kafkaStream = kafkaReader.getKafkaStream()

    val lines = kafkaStream.map(_._2).flatMap(str => {
      str.split("}}").map(_ + "}}")
    })

    val insertRecords = lines.filter(str => str.contains(Utils.insertOpt)).map(i => {
      Utils.insertRecordsConvert(i) match {
        case Some(s) => s
      }
    })

    val updateRecords = lines.filter(str => str.contains(Utils.updateOpt)).map(u => {
      Utils.updateRecordsConvert(u) match {
        case Some(m) => m
      }
    })

    insertRecords.foreachRDD(rdd => {

      HiveUtils.readBusinflagFromHive(sc, hvc)

      val entrust_details = hvc.read.json(rdd)

//      if (HiveUtils.schemaFieldsCheck(entrust_details.schema, "POSITION_STR", "FUND_ACCOUNT", "CLIENT_ID",
//        "CURR_DATE", "CURR_TIME", "FUND_CODE", "STOCK_NAME", "ENTRUST_PRICE",
//        "DEAL_SHARE", "BALANCE", "ENTRUST_STATUS", "BUSINESS_FLAG", "INIT_DATE")) {

      if (HiveUtils.schemaFieldsCheck(entrust_details.schema, "POSITION_STR", "FUND_ACCOUNT", "CLIENT_ID",
          "CURR_DATE", "CURR_TIME", "PROD_CODE", "PROD_NAME", "ENTRUST_PRICE",
          "BUSINESS_AMOUNT", "ENTRUST_BALANCE", "ENTRUST_STATUS", "BUSINESS_FLAG", "INIT_DATE")) {

        entrust_details.registerTempTable("entrust_details")

        hvc.udf.register("concatDateTime", Utils.concatDateTime2)

        val today = Utils.getSpecDay(0, "yyyyMMdd")
        val df = hvc.sql("select e.position_str, e.fund_account, e.client_id, " +
          "concatDateTime(e.curr_date, e.curr_time) as curr_time, " +
          "e.prod_code, e.prod_name, " +
          "COALESCE(bf.BUSINESS_NAME,'') as business_name, " +
          "e.entrust_price as price, " +
          "e.business_amount, " +
          "e.entrust_balance, " +
          "e.entrust_status, " +
          "e.init_date " +
          "from entrust_details e " +
          "left outer join tmp_businflag bf " +
          "on e.business_flag = bf.business_flag " +
          s"where e.init_date >= '${today}' and " +
          "e.position_str is not null and " +
          "e.fund_account is not null and " +
          "e.client_id is not null and " +
          "e.curr_date is not null and " +
          "e.curr_time is not null and " +
          "e.prod_code is not null and " +
          "e.prod_name is not null and " +
          "e.entrust_price is not null and " +
          "e.business_amount is not null and " +
          "e.entrust_balance is not null and " +
          "e.init_date is not null").repartition(2)
        //        df.show(10)

        df.foreachPartition(iter => {

          val hbaseConnect: Connection = HbaseUtils.getConnect()
          var jedisCluster: JedisCluster = null
          var tableDetails: Table = null
          var tableMapping: Table = null

          try {
            jedisCluster = new JedisCluster(Utils.jedisClusterNodes, 2000, 100, Utils.jedisConf)
            var tableName = TableName.valueOf(Utils.hbaseTOfentrustDetails)
            tableDetails = hbaseConnect.getTable(tableName)
            tableName = TableName.valueOf(Utils.hbaseTOfentrustMapping)
            tableMapping = hbaseConnect.getTable(tableName)

            for (r <- iter) {
              val key = String.format(Utils.redisClientRelKey, r(2).toString)
              val client = jedisCluster.hgetAll(key)

              if (!client.isEmpty) {
                //匹配客户-员工关系表中存在的记录
                val client_name = client.get("client_name")

                val mapper = new ObjectMapper()
                mapper.registerModule(DefaultScalaModule)
                val staff_list = mapper.readValue(client.get("staff_list"), classOf[util.ArrayList[immutable.Map[String, String]]])

                val position_str = r(0).toString
                val fund_account = r(1).toString
                val client_id = (math.BigInt(r(2).toString) + math.BigInt("100000000000000000000")).toString()
                val curr_time = r(3).toString
                val fund_code = r(4).toString
                val stock_name = r(5).toString
                val business_name = r(6).toString
                val price = r(7).toString
                val deal_share = r(8).toString
                val balance = r(9).toString
                val entrust_status = r(10).toString
                val init_date = Utils.initdateCvt(r(11).toString)

                for (i <- staff_list) {

                  val staff_id = i.getOrElse("id", "")
                  val staff_name = i.getOrElse("name", "")
                  val ts = (10000000000L - Utils.getUnixStamp(curr_time, "yyyy-MM-dd HH:mm:ss")).toString

                  //staff_id 逆序 同一员工下按position_str排序
                  val arr = Array(staff_id.reverse, ts, init_date, position_str, fund_account, fund_code)
                  val rowkey = arr.mkString(",")
                  val putTry = new Put(Bytes.toBytes(rowkey))
                  putTry.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("exist"), Bytes.toBytes("1"))

                  if (tableDetails.checkAndPut(Bytes.toBytes(rowkey), Bytes.toBytes("cf"), Bytes.toBytes("exist"), null, putTry)) {
                    //检验hbase无此明细 确保重提唯一性
                    //hbase 记录明细
                    val put = new Put(Bytes.toBytes(rowkey))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("position_str"), Bytes.toBytes(position_str))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("fund_account"), Bytes.toBytes(fund_account))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("client_id"), Bytes.toBytes(client_id))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("curr_time"), Bytes.toBytes(curr_time))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("fund_code"), Bytes.toBytes(fund_code))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("stock_name"), Bytes.toBytes(stock_name))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("business_name"), Bytes.toBytes(business_name))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("price"), Bytes.toBytes(price))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("deal_share"), Bytes.toBytes(deal_share))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("balance"), Bytes.toBytes(balance))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("entrust_status"), Bytes.toBytes(entrust_status))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("init_date"), Bytes.toBytes(init_date))

                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("client_name"), Bytes.toBytes(client_name))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("staff_id"), Bytes.toBytes(staff_id))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("staff_name"), Bytes.toBytes(staff_name))

                    tableDetails.put(put)

                    //记录postion_str与rowkey映射关系
                    val putMapping = new Put(Bytes.toBytes(position_str.reverse))
                    putMapping.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(rowkey), Bytes.toBytes("1"))
                    putMapping.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("init_date"), Bytes.toBytes(init_date))
                    tableMapping.put(putMapping)

                    //当日聚合统计
                    if (init_date == Utils.getSpecDay(0, "yyyy-MM-dd")) {
                      //记录条数汇总
                      jedisCluster.hincrBy(String.format(Utils.redisStaffInfoKey, staff_id), "ofentrust_count", 1)
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
            if (tableDetails != null)
              tableDetails.close()
            if (tableMapping != null)
              tableMapping.close()
            //            if (hbaseConnect != null)
            //              hbaseConnect.close()
          }
        })
      }
    })

    updateRecords.foreachRDD(rdd => {
      val rdd1 = rdd.filter(m => (m.contains("ENTRUST_STATUS") || m.contains("ENTRUST_PRICE") ||
        m.contains("BUSINESS_AMOUNT") || m.contains("ENTRUST_BALANCE")) && m.contains("POSITION_STR"))
      //相同postion_str 到一个分区 对应操作按pos排序
      val rdd2 = rdd1.map(m => (m("POSITION_STR")._1, m)).groupByKey(2).map(x => {
        (x._1, x._2.toList.sortWith((m1, m2) => {
          m1("pos")._1 < m2("pos")._1
        }))
      })

      rdd2.foreachPartition(iter => {

        val hbaseConnect: Connection = HbaseUtils.getConnect()
        var tableMapping: Table = null
        var tableDetails: Table = null

        try {
          //          hbaseConnect = HbaseUtils.getConnect()
          var tableName = TableName.valueOf(Utils.hbaseTOfentrustMapping)
          tableMapping = hbaseConnect.getTable(tableName)
          tableName = TableName.valueOf(Utils.hbaseTOfentrustDetails)
          tableDetails = hbaseConnect.getTable(tableName)

          for ((s, l) <- iter) {

            val rowkey = s.reverse
            val get = new Get(Bytes.toBytes(rowkey))
            val rst = tableMapping.get(get)

            if (!rst.isEmpty) {
              //对应关系
              val columns = rst.rawCells().map(c => Bytes.toString(c.getQualifierArray,
                c.getQualifierOffset,
                c.getQualifierLength)).filter(s => {s != "init_date"})
              for (m <- l) {
                //m对应一条update记录
                val entrust_status = m.get("ENTRUST_STATUS")
                val entrust_price = m.get("ENTRUST_PRICE")
                val deal_share = m.get("BUSINESS_AMOUNT")
                val balance = m.get("ENTRUST_BALANCE")

                columns.map(k => {
                  val put = new Put(Bytes.toBytes(k))
                  if (!entrust_status.isEmpty) {
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("entrust_status"), Bytes.toBytes(entrust_status.get._2))
                  }
                  if (!entrust_price.isEmpty) {
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("price"), Bytes.toBytes(entrust_price.get._2))
                  }
                  if (!deal_share.isEmpty) {
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("deal_share"), Bytes.toBytes(deal_share.get._2))
                  }
                  if (!balance.isEmpty) {
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("balance"), Bytes.toBytes(balance.get._2))
                  }
                  put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("last_update"), Bytes.toBytes(Utils.getSpecDay(0, "yyyy-MM-dd HH:mm:ss")))
                  tableDetails.put(put)
                })
              }
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
        }
      })
    })
  }

  def ctstentrustPrc(sc: SparkContext, ssc: StreamingContext, hvc: HiveContext): Unit = {
    val kafkaReader = new KafkaReader[String, String, StringDecoder, StringDecoder](ssc, Utils.brokerList, Utils.topicOggCtstentrust,
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

      val entrust_details = hvc.read.json(rdd)

      if (HiveUtils.schemaFieldsCheck(entrust_details.schema, "POSITION_STR", "BRANCH_NO", "FUND_ACCOUNT", "CLIENT_ID",
        "CURR_DATE", "CURR_TIME", "STOCK_CODE", "ENTRUST_PRICE", "ENTRUST_AMOUNT", "EXCHANGE_TYPE",
        "OP_ENTRUST_WAY", "ENTRUST_BS", "INIT_DATE")) {

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
          "100.00 as entrust_price, " +
          "e.entrust_amount as entrust_amount, " +
          "round(100*e.entrust_amount,2) as entrust_balance, " +
          "COALESCE(ew.DICT_PROMPT,'') as op_entrust_way_name, " +
          "COALESCE(et.DICT_PROMPT,'') as market_name, " +
          "e.exchange_type as exchange_type, " +
          "e.init_date as init_date " +
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
          "on mt.subentry = c.money_type " +
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

          var jedisCluster: JedisCluster = null
          val hbaseConnect: Connection = HbaseUtils.getConnect()
          var table: Table = null

          try {
            jedisCluster = new JedisCluster(Utils.jedisClusterNodes, 2000, 100, Utils.jedisConf)
            val tableName = TableName.valueOf(Utils.hbaseTCtstentrustDetails)
            table = hbaseConnect.getTable(tableName)

            for (r <- iter) {
              val key = String.format(Utils.redisClientRelKey, r(3).toString)
              val client = jedisCluster.hgetAll(key)

              if (!client.isEmpty) {
                //匹配客户-员工关系表中存在的记录
                val client_name = client.get("client_name")

                val mapper = new ObjectMapper()
                mapper.registerModule(DefaultScalaModule)
                val staff_list = mapper.readValue(client.get("staff_list"), classOf[util.ArrayList[immutable.Map[String, String]]])

                val position_str = r(0).toString
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
                val init_date = Utils.initdateCvt(r(15).toString)

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
                  val arr = Array(staff_id.reverse, ts, init_date, position_str, client_name, fund_account)
                  val rowkey = arr.mkString(",")
                  val putTry = new Put(Bytes.toBytes(rowkey))
                  putTry.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("exist"), Bytes.toBytes("1"))

                  if (table.checkAndPut(Bytes.toBytes(rowkey), Bytes.toBytes("cf"), Bytes.toBytes("exist"), null, putTry)) {
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
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("init_date"), Bytes.toBytes(init_date))

                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("client_name"), Bytes.toBytes(client_name))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("staff_id"), Bytes.toBytes(staff_id))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("staff_name"), Bytes.toBytes(staff_name))

                    table.put(put)

                    //当日聚合统计
                    if (init_date == Utils.getSpecDay(0, "yyyy-MM-dd")) {
                      //记录条数汇总
                      jedisCluster.hincrBy(String.format(Utils.redisStaffInfoKey, staff_id), "ctstentrust_count", 1)
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
