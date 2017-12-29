package com.citics.cdh.realtime

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
object StockjourDetails {

  val conf = new SparkConf().setAppName("crmClientOutline_stockjourDetails")
  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))
    val hvc = new HiveContext(sc)

    sc.setLogLevel("WARN")
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
                                      "CURR_DATE", "CURR_TIME", "OCCUR_AMOUNT", "BUSINESS_FLAG", "MONEY_TYPE", "STOCK_TYPE")) {

        stockjour_details.registerTempTable("stockjour_details")

        hvc.sql("select * from tmp_sysdict WHERE dict_entry = 1101").registerTempTable("tmp_moneytype")
        hvc.udf.register("concatDateTime", Utils.concatDateTime2)

        val df = hvc.sql("select s.position_str, s.branch_no, s.fund_account, s.client_id, " +
                         "concatDateTime(s.curr_date, s.curr_time) as curr_time, " +
                         "COALESCE(br.branch_name,'') as branch_name, " +
                         "COALESCE(mt.DICT_PROMPT,'') as money_type_name, " +
                         "s.exchange_type, " +
                         "s.stock_code, " +
                         "s.occur_amount as occur_amount, " +
                         "bf.business_name as remark " +
                         "from stockjour_details s " +
                         "join tmp_allbranch br " +
                         "on s.branch_no = br.branch_no " +
                         "join tmp_businflag bf " +
                         "on s.business_flag = bf.business_flag " +
                         "join tmp_moneytype mt " +
                         "on s.money_type = mt.subentry " +
                         "where s.stock_type != '7' and " +
                         "s.business_flag in (3002,4008,4010) and " +
                         "s.position_str is not null and " +
                         "s.fund_account is not null and " +
                         "s.client_id is not null and " +
                         "s.curr_date is not null and " +
                         "s.curr_time is not null and " +
                         "s.branch_no is not null and " +
                         "s.occur_amount is not null and " +
                         "s.money_type is not null").repartition(10)
        df.show(10)

        df.foreachPartition(iter => {

          var jedisCluster: JedisCluster = null
          var hbaseConnect: Connection = null
          var tableDetails: Table = null
          var tablePrice: Table = null

          try {
            jedisCluster = new JedisCluster(Utils.jedisClusterNodes, 2000, 100, Utils.jedisConf)
            hbaseConnect = HbaseUtils.getConnect()
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
                val client_id = r(3).toString
                val curr_time = r(4).toString
                val branch_name = r(5).toString
                val moneytype_name = r(6).toString
                val exchange_type = r(7).toString
                val stock_code = r(8).toString
                val occur_amount = r(9).toString
                val remark = r(10).toString

                val keyPrice = s"${exchange_type}|${stock_code}"
                val get = new Get(Bytes.toBytes(keyPrice))
                get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("LAST_PRICE"))
                val lastPrice = Bytes.toString(tablePrice.get(get).getValue(Bytes.toBytes("cf"), Bytes.toBytes("LAST_PRICE")))
                val out_asset = f"${lastPrice.toDouble * occur_amount.toDouble}%.2f"

                for (i <- staff_list) {

                  val staff_id = i.getOrElse("id", "")
                  val staff_name = i.getOrElse("name", "")

                  //staff_id 逆序 同一员工下按position_str排序
                  val arr = Array(staff_id.reverse, curr_time.split(" ")(0), position_str)
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

                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("client_name"), Bytes.toBytes(client_name))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("staff_id"), Bytes.toBytes(staff_id))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("staff_name"), Bytes.toBytes(staff_name))

                    tableDetails.put(put)

                    //当日聚合统计
                    if (curr_time.split(" ")(0) == Utils.getSpecDay(0, "yyyy-MM-dd")) {
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
