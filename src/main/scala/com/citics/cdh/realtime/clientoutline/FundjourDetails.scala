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

import scala.collection.JavaConversions._
import scala.collection.immutable

/**
  * Created by 029188 on 2017-12-5.
  */
object FundjourDetails {

  val conf = new SparkConf().setAppName("crmClientOutline_fundjourDetails")
  val logger = LoggerFactory.getLogger(getClass)

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext(conf)
    val ssc = new StreamingContext(sc, Seconds(10))
    val hvc = new HiveContext(sc)

    sc.setLogLevel("WARN")
    val kafkaReader = new KafkaReader[String, String, StringDecoder, StringDecoder](ssc, Utils.brokerList, Utils.topicOggFoudjour,
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
      HiveUtils.readSystemdictFromHive(sc, hvc)
      HiveUtils.readBankargFromHive(sc, hvc)
      HiveUtils.readBranchFromHive(sc, hvc)

      val fundjour_details = hvc.read.json(rdd)

      if (HiveUtils.schemaFieldsCheck(fundjour_details.schema, "POSITION_STR", "BRANCH_NO", "FUND_ACCOUNT", "CLIENT_ID",
                                      "CURR_DATE", "CURR_TIME", "POST_BALANCE", "OCCUR_BALANCE", "BANK_NO", "BUSINESS_FLAG",
                                      "BRANCH_NO", "OP_ENTRUST_WAY", "MONEY_TYPE", "INIT_DATE")) {

        fundjour_details.registerTempTable("fundjour_details")

        hvc.sql("select * from tmp_sysdict WHERE dict_entry = 1201").registerTempTable("tmp_entrustway")
        hvc.sql("select * from tmp_sysdict WHERE dict_entry = 1101").registerTempTable("tmp_moneytype")
        hvc.udf.register("concatDateTime", Utils.concatDateTime2)

        val today = Utils.getSpecDay(0, "yyyyMMdd")
        val df = hvc.sql("select f.position_str, f.branch_no, f.fund_account, f.client_id, " +
                         "concatDateTime(f.curr_date, f.curr_time) as curr_time, " +
                         "COALESCE(mt.DICT_PROMPT,'') as money_type_name, " +
                         "COALESCE(bf.business_name,'') as remark, " +
                         "f.post_balance as post_balance, " +
                         "f.occur_balance as occur_balance, " +
                         "COALESCE(ew.DICT_PROMPT,'') as op_entrust_way_name, " +
                         "f.bank_no as bank_no, " +
                         "COALESCE(bk.bank_name,'') as bank_name, " +
                         "f.business_flag as business_flag, " +
                         "COALESCE(br.branch_name,'') as branch_name, " +
                         "f.init_date " +
                         "from fundjour_details f " +
                         "left outer join tmp_allbranch br " +
                         "on f.branch_no = br.branch_no " +
                         "left outer join tmp_bankarg bk " +
                         "on f.bank_no = bk.bank_no " +
                         "left outer join tmp_businflag bf " +
                         "on f.business_flag = bf.business_flag " +
                         "left outer join tmp_entrustway ew " +
                         "on f.op_entrust_way = ew.subentry " +
                         "left outer join tmp_moneytype mt " +
                         "on f.money_type = mt.subentry " +
                         "where f.business_flag not in (2317, 2318, 4170) and " +
                         s"f.init_date >= '${today}' and " +
                         "f.position_str is not null and " +
                         "f.branch_no is not null and " +
                         "f.fund_account is not null and " +
                         "f.client_id is not null and " +
                         "f.curr_date is not null and " +
                         "f.curr_time is not null and " +
                         "f.post_balance is not null and " +
                         "f.occur_balance is not null and " +
                         "f.bank_no is not null and " +
                         "f.business_flag is not null and " +
                         "f.init_date is not null").repartition(4)

        df.foreachPartition(iter => {

          var jedisCluster: JedisCluster = null
          val hbaseConnect: Connection = HbaseUtils.getConnect()
          var table: Table = null

          try {
            jedisCluster = new JedisCluster(Utils.jedisClusterNodes, 2000, 100, Utils.jedisConf)
            val tableName = TableName.valueOf(Utils.hbaseTFoudjourDetails)
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
                val moneytype_name = r(5).toString
                val remark = r(6).toString
                val post_balance = r(7).toString
                val occur_balance = r(8).toString
                val op_entrust_way_name = r(9).toString
                val bank_no = r(10).toString
                val bank_name = r(11).toString
                val business_flag = r(12).toString
                val branch_name = r(13).toString
                val init_date = Utils.initdateCvt(r(14).toString)

                for (i <- staff_list) {

                  val staff_id = i.getOrElse("id", "")
                  val staff_name = i.getOrElse("name", "")
                  val ts = (10000000000L - Utils.getUnixStamp(curr_time, "yyyy-MM-dd HH:mm:ss")).toString

                  //staff_id 逆序 同一员工下按position_str排序
                  val arr = Array(staff_id.reverse, ts, init_date, position_str, fund_account)
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
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("moneytype_name"), Bytes.toBytes(moneytype_name))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("remark"), Bytes.toBytes(remark))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("post_balance"), Bytes.toBytes(post_balance))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("occur_balance"), Bytes.toBytes(occur_balance))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("op_entrust_way_name"), Bytes.toBytes(op_entrust_way_name))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("bank_no"), Bytes.toBytes(bank_no))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("bank_name"), Bytes.toBytes(bank_name))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("business_flag"), Bytes.toBytes(business_flag))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("branch_name"), Bytes.toBytes(branch_name))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("init_date"), Bytes.toBytes(init_date))

                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("client_name"), Bytes.toBytes(client_name))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("staff_id"), Bytes.toBytes(staff_id))
                    put.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("staff_name"), Bytes.toBytes(staff_name))

                    table.put(put)

                    //当日聚合统计
                    if (init_date == Utils.getSpecDay(0, "yyyy-MM-dd")) {
                      //记录条数汇总
                      jedisCluster.hincrBy(String.format(Utils.redisStaffInfoKey, staff_id), "fundjour_count", 1)

                      //实时汇总部分
                      val fundjourKey = String.format(Utils.redisAggregateFundjourKey, staff_id)

                      if (!jedisCluster.hexists(fundjourKey, "in_balance")) {
                        jedisCluster.hincrBy(fundjourKey, "in_count", 0)
                        jedisCluster.hincrByFloat(fundjourKey, "in_balance", 0.00)
                        jedisCluster.hincrBy(fundjourKey, "out_count", 0)
                        jedisCluster.hincrByFloat(fundjourKey, "out_balance", 0.00)
                        jedisCluster.hincrByFloat(fundjourKey, "balance", 0.00)

                        jedisCluster.expireAt(fundjourKey, Utils.getUnixStamp(Utils.getSpecDay(1, "yyyy-MM-dd"), "yyyy-MM-dd"))
                      }

                      if (business_flag == "2041" || business_flag == "2042") {
                        val occur = occur_balance.toDouble
                        if (occur > 0.000001) {
                          jedisCluster.hincrBy(fundjourKey, "in_count", 1)
                          jedisCluster.hincrByFloat(fundjourKey, "in_balance", occur)
                          jedisCluster.hincrByFloat(fundjourKey, "balance", occur)
                        }
                        if (occur < -0.000001) {
                          jedisCluster.hincrBy(fundjourKey, "out_count", 1)
                          jedisCluster.hincrByFloat(fundjourKey, "out_balance", occur)
                          jedisCluster.hincrByFloat(fundjourKey, "balance", occur)
                        }
                      }

                      //大于5w资金转出
                      if (business_flag == "2042") {
                        val fundoutKey = String.format(Utils.redisAggregateFundOutKey, staff_id)
                        val member = String.format("bno:%s:bname:%s:fund:%s:cn:%s:mt:%s:remark:%s:cid:%s",
                          branch_no, branch_name, fund_account, client_name, moneytype_name, remark, client_id)

                        if (jedisCluster.zcard(fundoutKey) == 0) {
                          jedisCluster.zincrby(fundoutKey, 0.00, member)
                          jedisCluster.expireAt(fundoutKey, Utils.getUnixStamp(Utils.getSpecDay(1, "yyyy-MM-dd"), "yyyy-MM-dd"))
                        }
                        jedisCluster.zincrby(fundoutKey, occur_balance.toDouble, member)
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
//            if (hbaseConnect != null)
//              hbaseConnect.close()
          }
        })
      }
    })

    ssc.start()
    ssc.awaitTermination()
  }
}
