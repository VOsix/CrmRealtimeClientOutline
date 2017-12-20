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
object OfentrustDetails {

  val conf = new SparkConf().setAppName("crmClientOutline_ofentrustDetails")
  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))
    val hvc = new HiveContext(sc)

    sc.setLogLevel("WARN")
    val kafkaReader = new KafkaReader[String, String, StringDecoder, StringDecoder](ssc, Utils.brokerList, Utils.topicOggOfentrust,
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

    val updateRecords = lines.filter(str => str.contains(Utils.updateOpt)).map(u => {
      Utils.updateRecordsConvert(u) match {
        case Some(m) => m
      }
    })

    insertRecords.foreachRDD(rdd => {

      HiveUtils.readBusinflagFromHive(sc, hvc)

      val entrust_details = hvc.read.json(rdd)

      if (HiveUtils.schemaFieldsCheck(entrust_details.schema, "POSITION_STR", "FUND_ACCOUNT", "CLIENT_ID",
                                      "CURR_DATE", "CURR_TIME", "FUND_CODE", "STOCK_NAME", "ENTRUST_PRICE",
                                      "DEAL_SHARE", "BALANCE", "ENTRUST_STATUS", "BUSINESS_FLAG")) {

        entrust_details.registerTempTable("entrust_details")

        hvc.udf.register("concatDateTime", Utils.concatDateTime2)

        val df = hvc.sql("select e.position_str, e.fund_account, e.client_id, " +
                         "concatDateTime(e.curr_date, e.curr_time) as curr_time, " +
                         "e.fund_code, e.stock_name, " +
                         "COALESCE(bf.BUSINESS_NAME,'') as business_name, " +
                         "e.entrust_price as price, " +
                         "e.deal_share, " +
                         "e.balance, " +
                         "e.entrust_status " +
                         "from entrust_details e " +
                         "left outer join tmp_businflag bf " +
                         "on e.business_flag = bf.business_flag " +
                         "where e.position_str is not null and " +
                         "e.fund_account is not null and " +
                         "e.client_id is not null and " +
                         "e.curr_date is not null and " +
                         "e.curr_time is not null and " +
                         "e.fund_code is not null and " +
                         "e.stock_name is not null and " +
                         "e.entrust_price is not null and " +
                         "e.deal_share is not null and " +
                         "e.balance is not null").repartition(10)
//        df.show(10)

        df.foreachPartition(iter => {

          var jedisCluster: JedisCluster = null
          var hbaseConnect: Connection = null
          var tableDetails: Table = null
          var tableMapping: Table = null

          try {
            jedisCluster = new JedisCluster(Utils.jedisClusterNodes, 2000, 100, Utils.jedisConf)
            hbaseConnect = HbaseUtils.getConnect()
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
                val client_id = r(2).toString
                val curr_time = r(3).toString
                val fund_code = r(4).toString
                var stock_name = r(5).toString
                var business_name = r(6).toString
                val price = r(7).toString
                val deal_share = r(8).toString
                val balance = r(9).toString
                val entrust_status = r(10).toString

                for (i <- staff_list) {

                  val staff_id = i.getOrElse("id", "")
                  val staff_name = i.getOrElse("name", "")

                  //staff_id 逆序 同一员工下按position_str排序
                  val arr = Array(staff_id.reverse, curr_time.split(" ")(0), position_str, fund_account, fund_code)
                  val rowkey = arr.mkString("|")
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

                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("client_name"), Bytes.toBytes(client_name))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("staff_id"), Bytes.toBytes(staff_id))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("staff_name"), Bytes.toBytes(staff_name))

                    tableDetails.put(put)

                    //记录postion_str与rowkey映射关系
                    val putMapping = new Put(Bytes.toBytes(position_str))
                    putMapping.addColumn(Bytes.toBytes("cf"), Bytes.toBytes(rowkey), Bytes.toBytes("1"))
                    tableMapping.put(putMapping)

                    //当日聚合统计
                    if (curr_time.split(" ")(0) == Utils.getSpecDay(0, "yyyy-MM-dd")) {
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
            if (hbaseConnect != null)
              hbaseConnect.close()
          }
        })
      }
    })

    updateRecords.foreachRDD(rdd => {
      val rdd1 = rdd.filter(m => (m.contains("ENTRUST_STATUS") || m.contains("ENTRUST_PRICE") ||
                                  m.contains("DEAL_SHARE") || m.contains("BALANCE")) && m.contains("POSITION_STR"))
      //相同postion_str 到一个分区 对应操作按pos排序
      val rdd2 = rdd1.map(m => (m("POSITION_STR")._1, m)).groupByKey(10).map(x => {
        (x._1, x._2.toList.sortWith((m1, m2) => {
          m1("pos")._1 < m2("pos")._1
        }))
      })

      rdd2.foreachPartition(iter => {

        var hbaseConnect: Connection = null
        var tableMapping: Table = null
        var tableDetails: Table = null

        try {
          hbaseConnect = HbaseUtils.getConnect()
          var tableName = TableName.valueOf(Utils.hbaseTOfentrustMapping)
          tableMapping = hbaseConnect.getTable(tableName)
          tableName = TableName.valueOf(Utils.hbaseTOfentrustDetails)
          tableDetails = hbaseConnect.getTable(tableName)

          for ((s, l) <- iter) {

            println(s"${s}: ${l}")
            val rowkey = s
            val get = new Get(Bytes.toBytes(rowkey))
            val rst = tableMapping.get(get)

            if (!rst.isEmpty) {
              //对应关系
              val columns = rst.rawCells().map(c => Bytes.toString(c.getQualifierArray, c.getQualifierOffset, c.getQualifierLength))

              for (m <- l) {
                //m对应一条update记录
                val entrust_status = m.get("ENTRUST_STATUS")
                val entrust_price = m.get("ENTRUST_PRICE")
                val deal_share = m.get("DEAL_SHARE")
                val balance = m.get("BALANCE")

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
                  tableDetails.put(put)
                })
              }
            } else {
              logger.warn(s"position_str: ${rowkey} not find")
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
          if (hbaseConnect != null)
            hbaseConnect.close()
        }
      })
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
