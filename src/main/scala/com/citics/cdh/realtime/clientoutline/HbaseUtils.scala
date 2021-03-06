package com.citics.cdh.realtime.clientoutline

import java.util.concurrent.Executors

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.slf4j.LoggerFactory

import scala.collection.mutable
import scala.collection.mutable.ListBuffer

/**
  * Created by 029188 on 2017-11-30.
  */
object HbaseUtils {

  val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.property.clientPort", Utils.hbasePort)
  conf.set("hbase.zookeeper.quorum", Utils.hbaseHosts)

  var conn: Connection = null
  val logger = LoggerFactory.getLogger(getClass)

  val stocks = mutable.HashMap[String, (String, String, String)]()

  def getConnect(): Connection = {
    if (conn == null) {
      logger.warn("create hbase connect")
      val pool = Executors.newFixedThreadPool(5)
      try {
        conn = ConnectionFactory.createConnection(conf, pool)
      } catch {
        case ex: Exception => {
          ex.printStackTrace()
          throw ex
        }
      }
    }
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

  def getStkcodeFromHbase(conn: Connection, exchange_type: String, stock_code: String): (String, String, String) = {

    var result: (String, String, String) = null
    val key = s"${exchange_type}|${stock_code}"
    val stock = stocks.get(key)
//    println(stocks.toString())

    if (stock.isEmpty) {
      //查询hbase
      val tableName = TableName.valueOf(Utils.hbaseTStkcode)
      val table = conn.getTable(tableName)

      val rowkey = key
      val get = new Get(Bytes.toBytes(rowkey))
      get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("STOCK_NAME"))
      get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("MONEY_TYPE"))
      get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("STOCK_TYPE"))

      try {
        val rs = table.get(get).rawCells().map(c => (Bytes.toString(c.getQualifierArray, c.getQualifierOffset, c.getQualifierLength),
                                                     Bytes.toString(c.getValueArray, c.getValueOffset, c.getValueLength))).toMap
        result = (rs.getOrElse("STOCK_NAME", ""), rs.getOrElse("MONEY_TYPE", "") match {
          case "0" => "人民币"
          case "1" => "美元"
          case "2" => "港币"
          case _ => ""
        }, rs.getOrElse("STOCK_TYPE", ""))
        logger.warn(s"stkcode read from hbase $exchange_type|$stock_code|${result._1}|${result._2}|${result._3}")

        if (result._1.length > 0 && result._2.length > 0 && result._3.length > 0) {
          stocks += (key->result)
        } else {
          logger.warn(s"hbase data error ${key}: ${result}")
        }
      } catch {
        case ex: Exception => {
          ex.printStackTrace()
          throw ex
        }
      } finally {
        table.close()
      }
    } else {
      result = stock.get
    }

    result
  }

  def getOptcodeFromHbase(conn: Connection, exchange_type: String, option_code: String): (String, String) = {

    val tableName = TableName.valueOf(Utils.hbaseTOptcode)
    val table = conn.getTable(tableName)

    val rowkey = s"${exchange_type}|${option_code}"
    val get = new Get(Bytes.toBytes(rowkey))
    get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("OPTION_NAME"))
    get.addColumn(Bytes.toBytes("cf"), Bytes.toBytes("MONEY_TYPE"))

    try {
      val rs = table.get(get).rawCells().map(c => (Bytes.toString(c.getQualifierArray, c.getQualifierOffset, c.getQualifierLength),
        Bytes.toString(c.getValueArray, c.getValueOffset, c.getValueLength))).toMap
      val result = (rs.getOrElse("OPTION_NAME", ""), rs.getOrElse("MONEY_TYPE", "") match {
        case "0" => "人民币"
        case "1" => "美元"
        case "2" => "港币"
        case _ => ""
      })
      logger.warn(s"optcode read from hbase $exchange_type|$option_code|${result._1}|${result._2}")
      result
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        throw ex
      }
    } finally {
      table.close()
    }
  }
}
