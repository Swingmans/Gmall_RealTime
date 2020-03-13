package com.atguigu.gmall.realtime.util

import java.util.Properties

import com.atguigu.gmall.common.constant.GmallConstant
import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

object RedisUtil {

  private var jedisPool: JedisPool = _

  def getClient: Jedis = {
    if (jedisPool == null) {
      val prop: Properties = PropertiesUtil.getProperties(GmallConstant.GMALL_REALTIME_CONFIG)
      val host: String = prop.getProperty("redis.host")
      val port: String = prop.getProperty("redis.port")
      val config: JedisPoolConfig = new JedisPoolConfig()
      config.setMaxTotal(100)  //最大连接数
      config.setMaxIdle(20)   //最大空闲
      config.setMinIdle(20)     //最小空闲
      config.setBlockWhenExhausted(true)  //忙碌时是否等待
      config.setMaxWaitMillis(500)//忙碌时等待时长 毫秒
      config.setTestOnBorrow(true)
      jedisPool = new JedisPool(config,host,port.toInt)
    }
    jedisPool.getResource
  }

}
