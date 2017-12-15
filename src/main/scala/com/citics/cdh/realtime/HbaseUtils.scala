package com.citics.cdh.realtime

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.{Connection, ConnectionFactory, Result, ResultScanner}
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.hive.HiveContext
import org.slf4j.LoggerFactory

import scala.collection.mutable.ListBuffer

/**
  * Created by 029188 on 2017-11-30.
  */
object HbaseUtils {

  val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.property.clientPort", Utils.hbasePort)
  conf.set("hbase.zookeeper.quorum", Utils.hbaseHosts)

  val logger = LoggerFactory.getLogger(getClass)

  def getConnect(): Connection = {
    val conn = ConnectionFactory.createConnection(conf)
    conn
  }

  //hbase scan结果转化为Json字符串列表
  def scanResultToJson(rstScan: ResultScanner): ListBuffer[String] = {
    var listBuffer = new ListBuffer[String]()
    val iter = rstScan.iterator()
    while(iter.hasNext) {
      //对应一行数据
      val row = iter.next()
      listBuffer.append(resultToJson(row))
    }
    listBuffer
  }

  //将一行对应的result类转化为json格式
  def resultToJson(rst: Result): String = {
    val cells = rst.rawCells()
    val kvs = cells.map(c => (Bytes.toString(c.getQualifierArray, c.getQualifierOffset, c.getQualifierLength),
      Bytes.toString(c.getValueArray, c.getValueOffset, c.getValueLength)))

    val str = kvs.map(s => {
      val fmt = "\"%s\":\"%s\""
      val formatted = fmt.format(s._1, s._2)
      formatted
    }).mkString(",")

    s"{${str}}"
  }

  def getDateFromHbase(sc: SparkContext, conf: Configuration): RDD[String] = {
    var rst: RDD[String] = null
    try {
      val rdd = sc.newAPIHadoopRDD(conf, classOf[TableInputFormat], classOf[ImmutableBytesWritable], classOf[Result])
      rst = rdd.map{
        case (_, result) => {
          resultToJson(result)}
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        logger.error("get date from hbase error, table: {}", conf.get(TableInputFormat.INPUT_TABLE))
      }
    }
    rst
  }
}
