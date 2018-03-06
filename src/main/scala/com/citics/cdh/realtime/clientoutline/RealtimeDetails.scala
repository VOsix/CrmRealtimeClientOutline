package com.citics.cdh.realtime.clientoutline

import java.util

import com.citics.cdh.kafkautils.KafkaReader
import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import kafka.serializer.StringDecoder
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client.{Connection, Put, Table}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.slf4j.LoggerFactory
import redis.clients.jedis.JedisCluster

import collection.JavaConversions._
import scala.collection.immutable

/**
  * Created by 029188 on 2017-11-30.
  */
object RealtimeDetails {

  val conf = new SparkConf().setAppName("crmClientOutline_realtimeDetails")
  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(20))
    val hvc = new HiveContext(sc)
    import hvc.implicits._

    sc.setLogLevel("WARN")
    val kafkaReader = new KafkaReader[String, String, StringDecoder, StringDecoder](ssc, Utils.brokerList, Utils.topicOggRealtime,
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
                         "where r.real_status != '2' and " +
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
                         "r.init_date is not null").repartition(40)
//        df.show(10)

        df.foreachPartition(iter => {

          var jedisCluster: JedisCluster = null
          var hbaseConnect: Connection = null
          var table: Table = null

          try {
            jedisCluster = new JedisCluster(Utils.jedisClusterNodes, 2000, 100, Utils.jedisConf)
            hbaseConnect = HbaseUtils.getConnect()
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
                    val arr = Array(staff_id.reverse, init_date, ts, position_str, client_name, fund_account, stkcode, real_name)
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
                        jedisCluster.hincrBy(String.format(Utils.redisStaffInfoKey, staff_id), "realtime_count", 1)

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
            if (hbaseConnect != null)
              hbaseConnect.close()
          }
        })
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
