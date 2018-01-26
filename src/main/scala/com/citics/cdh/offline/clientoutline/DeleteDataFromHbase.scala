package com.citics.cdh.offline.clientoutline


import java.util.{Calendar, Date}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._

import scala.collection.JavaConversions._
/**
  * Created by 029188 on 2018-1-8.
  */
object DeleteDataFromHbase {

  val conf = new SparkConf().setAppName("crmClientOutline_deleteDetails")

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext()
    sc.setLogLevel("WARN")

    val cal = Calendar.getInstance()
    cal.setTime(new Date())
    val dayOfWeek = cal.get(Calendar.DAY_OF_WEEK) - 1

    println(s"today is ${dayOfWeek}th day of week")

    if (dayOfWeek == 1) {
      //每周一清空历史明细
      deleteHbaseData(Utils.hbaseTRealtimeDetails)
      deleteHbaseData(Utils.hbaseTEntrustDetails)
      deleteHbaseData(Utils.hbaseTFoudjourDetails)
      deleteHbaseData(Utils.hbaseTCrdtrealtiemDetails)
      deleteHbaseData(Utils.hbaseTCrdtentrustDetails)
      deleteHbaseData(Utils.hbaseTOptrealtimeDetails)
      deleteHbaseData(Utils.hbaseTOptentrustDetails)
      deleteHbaseData(Utils.hbaseTCtstentrustDetails)
      deleteHbaseData(Utils.hbaseTOfentrustDetails)
      deleteHbaseData(Utils.hbaseTOtcbookorderDetails)
      deleteHbaseData(Utils.hbaseTOtcorderDetails)
      deleteHbaseData(Utils.hbaseTStockjourDetails)
      deleteHbaseData(Utils.hbaseTEntrustMapping)
      deleteHbaseData(Utils.hbaseTOfentrustMapping)
    }
  }

  def deleteHbaseData(tn: String): Unit = {

    println(s"start truncate ${tn}...")
    var hbaseConnect: Connection = null
    var admin: Admin = null

    try {
      hbaseConnect = Utils.getHbaseConn()
      val tableName = TableName.valueOf(tn)
      admin = hbaseConnect.getAdmin

      admin.disableTable(tableName)
      admin.truncateTable(tableName, true)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
      }
    } finally {
      if (admin != null)
        admin.close()
      if (hbaseConnect != null)
        hbaseConnect.close()
    }
  }
}
