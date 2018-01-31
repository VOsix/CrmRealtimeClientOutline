package com.citics.cdh.offline.clientoutline


import java.util
import java.util.{Calendar, Date}

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes

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

    deleteData(Utils.hbaseTRealtimeDetails, 0)
    deleteData(Utils.hbaseTEntrustDetails, 0)
    deleteData(Utils.hbaseTFoudjourDetails, 0)
    deleteData(Utils.hbaseTCrdtrealtiemDetails, 0)
    deleteData(Utils.hbaseTCrdtentrustDetails, 0)
    deleteData(Utils.hbaseTOptrealtimeDetails, 0)
    deleteData(Utils.hbaseTOptentrustDetails, 0)
    deleteData(Utils.hbaseTCtstentrustDetails, 0)
    deleteData(Utils.hbaseTOfentrustDetails, 0)
    deleteData(Utils.hbaseTOtcbookorderDetails, 0)
    deleteData(Utils.hbaseTOtcorderDetails, 0)
    deleteData(Utils.hbaseTStockjourDetails, 0)

    deleteData(Utils.hbaseTEntrustMapping, 1)
    deleteData(Utils.hbaseTCrdtentrustMapping, 1)
    deleteData(Utils.hbaseTOfentrustMapping, 1)
  }

  def deleteData(tn: String, ttype: Int): Unit = {

    println(s"start truncate ${tn}...")
    var hbaseConnect: Connection = null
    var admin: Admin = null

    try {
      hbaseConnect = Utils.getHbaseConn()
      admin = hbaseConnect.getAdmin
      val tableTmp = TableName.valueOf(Utils.hbaseTTmp)
      val tableName = TableName.valueOf(tn)

      //临时表清空
      admin.disableTable(tableTmp)
      admin.truncateTable(tableTmp, true)

      ttype match {
        case 0 => {
          moveData(tn, Utils.hbaseTTmp, hbaseConnect, detailsFilters)
          //明细表清空
          admin.disableTable(tableName)
          admin.truncateTable(tableName, true)

          moveData(Utils.hbaseTTmp, tn, hbaseConnect, null)
        }
        case 1 => {
          moveData(tn, Utils.hbaseTTmp, hbaseConnect, mappingFilters)
          //明细表清空
          admin.disableTable(tableName)
          admin.truncateTable(tableName, true)

          moveData(Utils.hbaseTTmp, tn, hbaseConnect, null)
        }
        case _ => {}
      }

      //临时表清空
      admin.disableTable(tableTmp)
      admin.truncateTable(tableTmp, true)
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        throw ex
      }
    } finally {
      if (admin != null)
        admin.close()
      if (hbaseConnect != null)
        hbaseConnect.close()
    }
  }

  def detailsFilters(): FilterList = {
    val pattern = s".*${Utils.getSpecDay(0, "yyyy-MM-dd")}.*"
    val regexFilter = new RowFilter(CompareFilter.CompareOp.EQUAL, new RegexStringComparator(pattern))

    val filters = new util.ArrayList[Filter]()
    filters.add(regexFilter)
    new FilterList(FilterList.Operator.MUST_PASS_ALL, filters)
  }

  def mappingFilters(): FilterList = {
    val singleColumnValueFilter = new SingleColumnValueFilter(
      Bytes.toBytes("cf"), Bytes.toBytes("init_date"),
      CompareFilter.CompareOp.EQUAL,
      new BinaryComparator(Bytes.toBytes(Utils.getSpecDay(0, "yyyy-MM-dd"))))
    singleColumnValueFilter.setFilterIfMissing(true)

    val filters = new util.ArrayList[Filter]()
    filters.add(singleColumnValueFilter)
    new FilterList(FilterList.Operator.MUST_PASS_ALL, filters)
  }

  def moveData(from: String, to: String, conn: Connection, filterList: FilterList): Unit = {

    println(s"moving ${from} data to ${to}")
    var tableFrom: Table = null
    var tableTo: Table = null
    var rs: ResultScanner = null

    try {
      val fname = TableName.valueOf(from)
      val tname = TableName.valueOf(to)

      tableFrom = conn.getTable(fname)
      tableTo = conn.getTable(tname)

      val scan = new Scan()
      scan.addFamily(Bytes.toBytes("cf"))
      if (filterList != null)
        scan.setFilter(filterList)
      rs = tableFrom.getScanner(scan)

      val iter = rs.iterator()
      while (iter.hasNext) {
        val result = iter.next()

        val rowkey = result.getRow
        val puts = new util.ArrayList[Put]()

        for (c <- result.rawCells()) {
          val put = new Put(rowkey)

          val family = Bytes.toString(c.getFamilyArray, c.getFamilyOffset, c.getFamilyLength)
          val qualifier = Bytes.toString(c.getQualifierArray, c.getQualifierOffset, c.getQualifierLength)
          val value = Bytes.toString(c.getValueArray, c.getValueOffset, c.getValueLength)

          put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value))
          puts.add(put)
        }

        tableTo.put(puts)
      }
    } catch {
      case ex: Exception => {
        ex.printStackTrace()
        throw ex
      }
    } finally {
      if (rs != null)
        rs.close()
      if (tableFrom != null)
        tableFrom.close()
      if (tableTo != null)
        tableTo.close()
    }
  }
}
