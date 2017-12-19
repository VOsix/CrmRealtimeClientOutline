package com.citics.cdh.realtime

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

import scala.collection.JavaConversions._
import scala.collection.immutable

/**
  * Created by 029188 on 2017-12-5.
  */
object OtcbookordersDetail {

  val conf = new SparkConf().setAppName("crmClientOutline_otcbookordersDetail")
  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))
    val hvc = new HiveContext(sc)

    sc.setLogLevel("WARN")
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
                         "orderTypeConvert(e.trd_id) as ord_type, " +
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
                         "e.book_timestamp is not null")

        df.foreachPartition(iter => {

          var jedisCluster: JedisCluster = null
          var hbaseConnect: Connection = null
          var table: Table = null

          try {
            jedisCluster = new JedisCluster(Utils.jedisClusterNodes, 2000, 100, Utils.jedisConf)
            hbaseConnect = HbaseUtils.getConnect()
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
                val client_id = r(1).toString
                val fund_account = r(2).toString
                val fund_code = r(3).toString
                val fund_name = r(4).toString
                val amt = r(5).toString
                val ord_type = r(6).toString
                val ord_time = r(7).toString

                for (i <- staff_list) {

                  val staff_id = i.getOrElse("id", "")
                  val staff_name = i.getOrElse("name", "")

                  //staff_id 逆序 同一员工下按position_str排序
                  val arr = Array(staff_id.reverse, ord_time.split(" ")(0), position_str, fund_account)
                  val rowkey = arr.mkString("|")
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

        df.unpersist()
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }

}
