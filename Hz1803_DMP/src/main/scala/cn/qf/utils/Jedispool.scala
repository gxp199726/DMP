package cn.qf.utils

import redis.clients.jedis.{Jedis, JedisPool, JedisPoolConfig}

/**
  * 连接池
  */
object Jedispool {
  val config = new JedisPoolConfig()
  //设置最大连接
  config.setMaxTotal(20)
  config.setMaxIdle(10)
  //连接等待时长
  private val pool = new JedisPool(config,"192.168.153.200",6379,10000,"123")
  //获取连接
  def getConnection():Jedis={
    pool.getResource
  }
}
