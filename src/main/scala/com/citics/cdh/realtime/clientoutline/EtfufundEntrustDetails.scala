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
object EtfufundEntrustDetails {

  val conf = new SparkConf().setAppName("crmClientOutline_etfufundentrustDetails")
  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(20))
    val hvc = new HiveContext(sc)

    sc.setLogLevel("WARN")
    val kafkaReader = new KafkaReader[String, String, StringDecoder, StringDecoder](ssc, Utils.brokerList, Utils.topicOggEtfufundentrust,
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

    insertRecords.foreachRDD(rdd => {

      HiveUtils.readStkcodeFromHive(sc, hvc)
      HiveUtils.readSystemdictFromHive(sc, hvc)

      val entrust_details = hvc.read.json(rdd)

      if (HiveUtils.schemaFieldsCheck(entrust_details.schema, "POSITION_STR", "BRANCH_NO", "FUND_ACCOUNT", "CLIENT_ID",
                                      "CURR_DATE", "CURR_TIME", "STOCK_CODE", "ENTRUST_PRICE", "ENTRUST_AMOUNT", "EXCHANGE_TYPE",
                                      "OP_ENTRUST_WAY", "ENTRUST_BS", "STOCK_TYPE", "INIT_DATE", "STOCK_NAME")) {

        entrust_details.registerTempTable("entrust_details")

        hvc.sql("select * from tmp_sysdict WHERE dict_entry = 1201").registerTempTable("tmp_entrustway")
        hvc.sql("select * from tmp_sysdict WHERE dict_entry = 1204").registerTempTable("tmp_entrustbs")
        hvc.sql("select * from tmp_sysdict WHERE dict_entry = 1301").registerTempTable("tmp_exchangetype")
        hvc.sql("select * from tmp_sysdict WHERE dict_entry = 1101").registerTempTable("tmp_moneytype")
        hvc.udf.register("concatDateTime", Utils.concatDateTime2)

        val today = Utils.getSpecDay(0, "yyyyMMdd")
        val df = hvc.sql("select e.position_str, e.branch_no, e.fund_account, e.client_id, " +
                         "concatDateTime(e.curr_date, e.curr_time) as curr_time, " +
                         "e.stock_code as stkcode, COALESCE(e.stock_name,'') as stkname, " +
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
                         "e.position_str is not null and " +
                         "e.branch_no is not null and " +
                         "e.fund_account is not null and " +
                         "e.client_id is not null and " +
                         "e.curr_date is not null and " +
                         "e.curr_time is not null and " +
                         "e.stock_code is not null and " +
                         "e.entrust_price is not null and " +
                         "e.entrust_amount is not null and " +
                         "e.init_date is not null")

        df.foreachPartition(iter => {

          val hbaseConnect: Connection = HbaseUtils.getConnect()
          var jedisCluster: JedisCluster = null
          var tableDetails: Table = null

          try {
            jedisCluster = new JedisCluster(Utils.jedisClusterNodes, 2000, 100, Utils.jedisConf)
            val tableName = TableName.valueOf(Utils.hbaseTEntrustDetails)
            tableDetails = hbaseConnect.getTable(tableName)

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
                val stock_type = r(15).toString
                val init_date = Utils.initdateCvt(r(16).toString)

                println(r.mkString("|"))

                if (stkname.length == 0 || moneytype_name.length == 0) {
                  //通过hbase查询
                  val stockInfo = HbaseUtils.getStkcodeFromHbase(hbaseConnect, exchange_type, stkcode)

                  if (stkname.length == 0)
                    stkname = stockInfo._1
                  if (moneytype_name.length == 0)
                    moneytype_name = stockInfo._2
                }

                val details_puts = new util.ArrayList[Put]()

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


                    details_puts.add(put)
                    if (details_puts.size() == 100) {
                      //100条提交一次
                      tableDetails.put(details_puts)
                      details_puts.clear()
                      logger.warn("details hbase commit...")
                    }

                    //记录条数汇总
                    jedisCluster.hincrBy(String.format(Utils.redisStaffInfoKey, staff_id), "entrust_count", 1)

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

                if (details_puts.size() > 0)
                  tableDetails.put(details_puts)
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
          }
        })
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
