package com.citics.cdh.realtime.outline

import java.util

import redis.clients.jedis.{HostAndPort, JedisPoolConfig}

object Utils {

  val hbaseTRealtimeDetails = "realtime:client_outline_realtime_details"
  val hbaseTEntrustDetails  = "realtime:client_outline_entrust_details"
  val hbaseTFoudjourDetails  = "realtime:client_outline_fundjour_details"

  //redis
  val jedisConf = new JedisPoolConfig()
  jedisConf.setMaxTotal(100)
  jedisConf.setMaxIdle(50)
  jedisConf.setMinIdle(20)
  jedisConf.setMaxWaitMillis(6 * 1000)
  jedisConf.setTestOnBorrow(true)
  val jedisClusterNodes = new util.HashSet[HostAndPort]()
  jedisClusterNodes.add(new HostAndPort("10.23.152.236", 7000))
  jedisClusterNodes.add(new HostAndPort("10.23.152.236", 7001))
  jedisClusterNodes.add(new HostAndPort("10.23.152.237", 7000))
  jedisClusterNodes.add(new HostAndPort("10.23.152.237", 7001))
  jedisClusterNodes.add(new HostAndPort("10.23.152.239", 7000))
  jedisClusterNodes.add(new HostAndPort("10.23.152.239", 7001))
  jedisClusterNodes.add(new HostAndPort("10.23.152.240", 7000))
  jedisClusterNodes.add(new HostAndPort("10.23.152.240", 7001))
  val redisClientRelKey = "realtime:crm:client_rel:client_id:%s"
  val redisStaffInfoKey = "realtime:crm:staff_info:staff_id:%s"
  val redisAggregateRealtimeKey = "realtime:crm:aggregate:realtime:staff_id:%s"
  val redisAggregateTopdealKey  = "realtime:crm:aggregate:realtime:top10:staff_id:%s"
  val redisAggregateEntrustKey  = "realtime:crm:aggregate:entrust:staff_id:%s"
  val redisAggregateFundjourKey = "realtime:crm:aggregate:fundjour:staff_id:%s"
  val redisAggregateFundOutKey  = "realtime:crm:aggregate:fundjour:out:staff_id:%s"

  val redisRealtimeIndex = "realtime:crm:index:realtime:staff_id:%s"
  val redisEntrustIndex = "realtime:crm:index:entrust:staff_id:%s"
  val redisFoudjourIndex = "realtime:crm:index:fundjour:staff_id:%s"
}
