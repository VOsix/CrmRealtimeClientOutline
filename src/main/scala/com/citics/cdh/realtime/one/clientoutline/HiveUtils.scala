package com.citics.cdh.realtime.one.clientoutline

import org.apache.spark.SparkContext
import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.types.StructType
import org.slf4j.LoggerFactory

import scala.collection.mutable

/**
  * Created by 029188 on 2017-12-2.
  */
object HiveUtils {

  var hvc: HiveContext = null
  val logger = LoggerFactory.getLogger(getClass)

  var hasReadStkcode = false
  var hasReadOptcode = false
  var hasReadBranch = false
  var hasReadBankarg = false
  var hasReadBusinflag = false
  var hasReadSystemDict = false

  def query_init(sc: SparkContext): Unit = {
    if(hvc == null)
      hvc = new HiveContext(sc)
  }

  def readStkcodeFromHive(sc: SparkContext, hvc: HiveContext): Unit = {

    if (hasReadStkcode != true) {
      query_init(sc)
      val df = hvc.sql(s"select exchange_type, stock_code, stock_type, " +
                       s"COALESCE(stock_name,'') as stock_name, COALESCE(money_type,'') as money_type " +
                       s"from ${Utils.hiveStockCode}")
      df.registerTempTable("tmp_stkcode")
      hvc.cacheTable("tmp_stkcode")
      hasReadStkcode = true
      //cacheTable 需要action操作触发
      println(s"read ${df.count()} stkcode records from hive")
    }
  }

  def readOptcodeFromHive(sc: SparkContext, hvc: HiveContext): Unit = {

    if (hasReadOptcode != true) {
      query_init(sc)
      val df = hvc.sql(s"select exchange_type, option_code, " +
                       s"COALESCE(stock_name,'') as stock_name, COALESCE(money_type,'') as money_type " +
                       s"from ${Utils.hiveOptCode}")
      df.registerTempTable("tmp_optcode")
      hvc.cacheTable("tmp_optcode")
      hasReadOptcode = true
      println(s"read ${df.count()} optcode records from hive")
    }
  }

  def readBranchFromHive(sc: SparkContext, hvc: HiveContext): Unit = {

    if (hasReadBranch != true) {
      query_init(sc)
      val df = hvc.sql(s"select branch_no, COALESCE(branch_name,'') as branch_name " +
                       s"from ${Utils.hiveBranch}")
      df.registerTempTable("tmp_allbranch")
      hvc.cacheTable("tmp_allbranch")
      hasReadBranch = true
      println(s"read ${df.count()} allbranch records from hive")
    }
  }

  def readBankargFromHive(sc: SparkContext, hvc: HiveContext): Unit = {

    if (hasReadBankarg != true) {
      query_init(sc)
      val df = hvc.sql(s"select bank_no, COALESCE(bank_name,'') as bank_name " +
                       s"from ${Utils.hiveBankArg}")
      df.registerTempTable("tmp_bankarg")
      hvc.cacheTable("tmp_bankarg")
      hasReadBankarg = true
      println(s"read ${df.count()} bankarg records from hive")
    }
  }

  def readBusinflagFromHive(sc: SparkContext, hvc: HiveContext): Unit = {

    if (hasReadBusinflag != true) {
      query_init(sc)
      val df = hvc.sql(s"select business_flag, COALESCE(business_name,'') as business_name " +
                       s"from ${Utils.hiveBusFlag}")
      df.registerTempTable("tmp_businflag")
      hvc.cacheTable("tmp_businflag")
      hasReadBusinflag = true
      println(s"read ${df.count()} businflag records from hive")
    }
  }

  def readSystemdictFromHive(sc: SparkContext, hvc: HiveContext): Unit = {

    if (hasReadSystemDict != true) {
      query_init(sc)
      val df = hvc.sql(s"select dict_entry, subentry, dict_prompt " +
                       s"from ${Utils.hiveSystemDict} " +
                       s"where dict_entry in (1101, 1201, 1204, 1212, 1301)")
      df.registerTempTable("tmp_sysdict")
      hvc.cacheTable("tmp_sysdict")
      hasReadSystemDict = true
      println(s"read ${df.count()} sysdictionary records from hive")
    }
  }

  def schemaFieldsCheck(scehma: StructType, fnames: String*): Boolean = {
    var hasAllField = true
    val scehmaFields = scehma.fields.map(fd => (fd.name, 1)).toMap
    val fields = mutable.HashSet[String]()

    for(fn <- fnames) {
      val result = scehmaFields.getOrElse(fn, 0)
      if(result == 0) {
        hasAllField = false
        fields.add(fn)
      }
    }
    if (!hasAllField)
      logger.warn("{} not find in schema", fields)
    hasAllField
  }
}
