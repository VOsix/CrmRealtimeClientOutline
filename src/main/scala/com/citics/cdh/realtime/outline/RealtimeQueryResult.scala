package com.citics.cdh.realtime.outline

import java.util
import java.util.PriorityQueue

/**
  * Created by 029188 on 2017-12-8.
  */
case class RealtimeItem(fund_account: String, client_name: String, deal_time: String, stock_code: String,
                        stock_name: String, remark: String, ccy: String, deal_price: Double, deal_amout: Long,
                        deal_balance: Double, deal_type: String, exchange_type: String) {
}

class RealtimeQueryResult() {

  var errCode = ""
  var info = ""
  var count: Int = 0
  var total: Long = 0
  var results = new util.ArrayList[RealtimeItem]()
}
