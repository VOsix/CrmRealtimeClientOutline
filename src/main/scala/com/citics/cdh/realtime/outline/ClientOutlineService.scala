package com.citics.cdh.realtime.outline

import com.fasterxml.jackson.databind.ObjectMapper
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.hadoop.hbase.client.ConnectionFactory

/**
  * Created by 029188 on 2017-12-7.
  */
class ClientOutlineService {



  def queryRealtimeDetailFromHbase(staff_id: String, //员工号
                                   page_size: Int,   //页面条数
                                   roll_direct: Int, //翻页方向 0: 首次 1: 向前 2: 向后
                                   first_index: Long,//当前页面第一条数据索引
                                   last_index: Long, //当前页面最后一条数据索引
                                   client_name: String, //客户姓名
                                   deal_type: String,   //成交类型
                                   fund_account: String,//资金账号
                                   stk_code: String     //证券代码
                                  ): String = {

    val rst = new RealtimeQueryResult
    var rstStr = ""

    if (rst.errCode.length == 0 && staff_id.length == 0) {

      rst.info = "员工号不能为空"
      rst.errCode = "E0001"
      rstStr = ClientOutlineService.mapper.writeValueAsString(rst)
    }




    rstStr
  }







}

object ClientOutlineService {
  private val hbaseHosts = "10.23.147.32,10.23.147.33,10.23.147.39"
  private val hbasePort  = "2181"

  private val conf = HBaseConfiguration.create()
  conf.set("hbase.zookeeper.property.clientPort", hbasePort)
  conf.set("hbase.zookeeper.quorum", hbaseHosts)

  private val hbaseConnect = ConnectionFactory.createConnection(ClientOutlineService.conf)

  private val mapper = new ObjectMapper()
  mapper.registerModule(DefaultScalaModule)
}
