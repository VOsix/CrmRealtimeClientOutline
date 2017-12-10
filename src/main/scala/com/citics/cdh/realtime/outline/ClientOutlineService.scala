package com.citics.cdh.realtime.outline

import java.util

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.hbase.{HBaseConfiguration, TableName}
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.client.coprocessor.{AggregationClient, LongColumnInterpreter}
import org.apache.hadoop.hbase.filter._
import org.apache.hadoop.hbase.util.Bytes
import redis.clients.jedis.JedisCluster

import collection.JavaConversions._


/**
  * Created by 029188 on 2017-12-7.
  */
class ClientOutlineService {



  def queryRealtimeDetailFromHbase(staff_id: String, //员工号
                                   page_size: Int,   //页面条数
                                   page_num: Int,    //页码
                                   client_name: String, //客户姓名
                                   real_type: String,   //成交类型
                                   fund_account: String,//资金账号
                                   stkcode: String     //证券代码
                                  ): String = {

    val rst = new RealtimeQueryResult

    val checkRst = checkInput(staff_id, page_size, page_num)

    if (checkRst._1.length == 0) {

      var jedisCluster: JedisCluster = null
      var hbaseConnect: Connection = null
      var table: Table = null

      val filters = new util.ArrayList[Filter]()

      if (client_name.length > 0) {
        val filter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("client_name"),
          CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(client_name)))
        filters.add(filter)
      }

      if (real_type.length > 0) {
        val filter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("real_type"),
          CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(real_type)))
        filters.add(filter)
      }

      if (fund_account.length > 0) {
        val filter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("fund_account"),
          CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(fund_account)))
        filters.add(filter)
      }

      if (stkcode.length > 0) {
        val filter = new SingleColumnValueFilter(Bytes.toBytes("cf"), Bytes.toBytes("stkcode"),
          CompareFilter.CompareOp.EQUAL, new BinaryComparator(Bytes.toBytes(stkcode)))
        filters.add(filter)
      }

      val tableName = TableName.valueOf(Utils.hbaseTRealtimeDetails)

      try {

        jedisCluster = new JedisCluster(Utils.jedisClusterNodes, 2000, 100, Utils.jedisConf)
        hbaseConnect = ConnectionFactory.createConnection(ClientOutlineService.conf)
        table = hbaseConnect.getTable(tableName)
        val scan = new Scan()
        scan.addFamily(Bytes.toBytes("cf"))

        val begin = page_num * page_size
        val end = (page_num + 1) * page_size - 1

        if (filters.size() > 0) {
          //存在过滤条件
          val key = String.format(Utils.redisRealtimeIndex, staff_id)
          var count = 0
          var total = 0L

          val start = jedisCluster.zrange(key, 0, 0).head
          val stop = jedisCluster.zrange(key, -1, -1).head
          scan.setStartRow(Bytes.toBytes(start))
          scan.setStopRow(Bytes.toBytes(stop))

//          val pageFilter = new PageFilter((page_num+1) * page_size)
//          filters.add(pageFilter)
          val keyOnlyFilter = new KeyOnlyFilter()
          filters.add(keyOnlyFilter)
          val inclusiveStopFilter = new InclusiveStopFilter(Bytes.toBytes(stop))
          filters.add(inclusiveStopFilter)

          val filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL, filters)
          scan.setFilter(filterList)

//          val aggregation = new AggregationClient(ClientOutlineService.conf)
//          val total = aggregation.rowCount(table, new LongColumnInterpreter(), scan)

          val scanRst = table.getScanner(scan)

          for (r <- scanRst) {

            if (total >= begin && total <= end) {
              val map = resultToMap(r)

              //需再次查询
              rst.results.add(RealtimeItem(
                map.getOrElse("fund_account", ""),
                map.getOrElse("client_name", ""),
                map.getOrElse("curr_time", ""),
                map.getOrElse("stkcode", ""),
                map.getOrElse("stkname", ""),
                map.getOrElse("remark", ""),
                map.getOrElse("moneytype_name", ""),
                map.getOrElse("price", "0.00").toDouble,
                map.getOrElse("amount", "0").toLong,
                map.getOrElse("balance", "0.00").toDouble,
                map.getOrElse("real_name", ""),
                map.getOrElse("market_name", "")))
              count += 1
            }
            total += 1
          }
          rst.count = count
          rst.total = if (total % page_size == 0) {total / page_size} else {total / page_size + 1}
          scanRst.close()
        } else {
          //无其他过滤条件
          val key = String.format(Utils.redisRealtimeIndex, staff_id)
          var count = 0
          val total = jedisCluster.zcard(key)
          val start = jedisCluster.zrange(key, begin, begin).head
          val stop = jedisCluster.zrange(key, end, end).head
          scan.setStartRow(Bytes.toBytes(start))
          scan.setStopRow(Bytes.toBytes(stop))

          val inclusiveStopFilter = new InclusiveStopFilter(Bytes.toBytes(stop))
          scan.setFilter(inclusiveStopFilter)

          val scanRst = table.getScanner(scan)

          for (r <- scanRst) {
            val map = resultToMap(r)

            rst.results.add(RealtimeItem(
              map.getOrElse("fund_account", ""),
              map.getOrElse("client_name", ""),
              map.getOrElse("curr_time", ""),
              map.getOrElse("stkcode", ""),
              map.getOrElse("stkname", ""),
              map.getOrElse("remark", ""),
              map.getOrElse("moneytype_name", ""),
              map.getOrElse("price", "0.00").toDouble,
              map.getOrElse("amount", "0").toLong,
              map.getOrElse("balance", "0.00").toDouble,
              map.getOrElse("real_name", ""),
              map.getOrElse("market_name", "")))
            count += 1
          }
          rst.count = count
          rst.total = if (total % page_size == 0) {total / page_size} else {total / page_size + 1}
          scanRst.close()
        }
      } catch {
        case ex: Exception => {
          ex.printStackTrace()
        }
      } finally {
        if (table != null) {
          table.close()
        }
        if (hbaseConnect != null) {
          hbaseConnect.close()
        }
        if (jedisCluster != null) {
          jedisCluster.close()
        }
      }
    } else {
      rst.errCode = checkRst._1
      rst.info = checkRst._2
    }

    ClientOutlineService.mapper.writeValueAsString(rst)
  }

  def resultToMap(rst: Result): Map[String, String] = {
    val cells = rst.rawCells()
    var kvs = cells.map(c => (Bytes.toString(c.getQualifierArray, c.getQualifierOffset, c.getQualifierLength),
                              Bytes.toString(c.getValueArray, c.getValueOffset, c.getValueLength)))
    kvs.toMap
  }

  def checkInput(staff_id: String, page_size: Int, page_num: Int): (String, String) = {

    var rst = ("", "")
    var hasExcept = false

    if (!hasExcept && staff_id.length == 0) {
      rst = ("E0001", "员工号不能为空")
      hasExcept = true
    }

    if (!hasExcept && page_size <= 0) {
      rst = ("E0002", "页面条数小于等于0")
      hasExcept = true
    }

    if (!hasExcept && page_num <= 0) {
      rst = ("E0003", "页码小于等于0")
      hasExcept = true
    }

    rst
  }
}

object ClientOutlineService {
  private val hbaseHosts = "10.23.147.32,10.23.147.33,10.23.147.39"
  private val hbasePort  = "2181"

  private val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.property.clientPort", hbasePort)
  conf.set("hbase.zookeeper.quorum", hbaseHosts)

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
}
