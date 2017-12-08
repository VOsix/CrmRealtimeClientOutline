package com.citics.cdh.realtime.outline

import java.util

/**
  * Created by 029188 on 2017-12-8.
  */
case class RealtimeItem(fund_account: String, client_name: String, deal_time: String, stock_code: String,
                        stock_name: String, remark: String, ccy: String, deal_price: Double, deal_amout: Long,
                        deal_balance: Double, deal_type: String, exchange_type: String, index: Long) {
}

class RealtimeQueryResult() {

  var errCode: String = ""
  var info: String = ""
  var count: Long = 0
  var hasPre: String = "0"
  var hasNext: String = "0"
  var result: util.ArrayList[RealtimeItem] = new util.ArrayList[RealtimeItem]()
}
