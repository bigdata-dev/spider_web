package com.ryxc.spider.web.utils;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

public class RedisUtils {
	JedisPool jedisPool = null;
	public RedisUtils(){
		JedisPoolConfig poolConfig = new JedisPoolConfig();
		poolConfig.setMaxIdle(10);
		poolConfig.setMaxTotal(100);
		poolConfig.setMaxWaitMillis(10000);
		poolConfig.setTestOnBorrow(true);
		jedisPool = new JedisPool(poolConfig, "192.168.116.166", 6379);
	}
	public void add(String lowKey, String url) {
		Jedis resource = jedisPool.getResource();
		resource.lpush(lowKey, url);
		jedisPool.returnResource(resource);
	}
	public String poll(String key) {
		Jedis resource = jedisPool.getResource();
		String result = resource.lpop(key);
		jedisPool.returnResource(resource);
		return result;
	}
}
