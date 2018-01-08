package com.citics.cdh.offline.clientoutline


import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._

import scala.collection.JavaConversions._
/**
  * Created by 029188 on 2018-1-8.
  */
object DeleteDataFromHbase {

  val conf = new SparkConf().setAppName("DeleteDataFromHbase")

  def main(args: Array[String]): Unit = {

    val sc = new SparkContext()
    sc.setLogLevel("WARN")

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
