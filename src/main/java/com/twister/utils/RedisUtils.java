package com.twister.utils;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.JedisShardInfo;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
import com.twister.utils.AppsConfig;

/**
 * This abstract bolt class will help publishing the bolt processing into a
 * redis pubsub channel
 * 
 * @author guoqing
 * 
 */
public final class RedisUtils {
	protected final static Logger logger = LoggerFactory.getLogger(RedisUtils.class);
	
	public static JedisPool jedisPool;
	public static JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
	public static Jedis jedis;
	public static ShardedJedis shardedJedis;
	public static ShardedJedisPool shardedJedisPool;
	public static String host = "127.0.0.1";
	public static int port = 6379;
	public static List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
	public static String SlaveHost = AppsConfig.getInstance().getValue("redis.slave.host");
	public static int SlavePort = Integer.valueOf(AppsConfig.getInstance().getValue("redis.slave.port"));
	
	static {
		try {
			createRedis();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
	public RedisUtils() {
	}
	
	public static Jedis getJedis() {
		return jedis;
	}
	
	public static JedisPool getJedisPool() {
		return jedisPool;
	}
	
	public static ShardedJedisPool getShardedJedisPool() {
		return shardedJedisPool;
	}
	
	public static ShardedJedis getShardedJedis() {
		shardedJedis = shardedJedisPool.getResource();
		return shardedJedis;
	}
	
	public static ShardedJedisPool createRedis() {
		// 池基本配置
		jedisPoolConfig.setMaxActive(1024);
		jedisPoolConfig.setMaxIdle(200);
		jedisPoolConfig.setMaxWait(1000);
		jedisPoolConfig.setTestOnBorrow(true);
		jedisPoolConfig.setTestOnReturn(false);
		host = AppsConfig.getInstance().getValue("redis.master.host");
		port = Integer.valueOf(AppsConfig.getInstance().getValue("redis.master.port"));
		if (host != null) {
			host = "127.0.0.1";
		}
		port = port > 0 ? port : 6379;
		jedisPool = new JedisPool(jedisPoolConfig, host, port);
		setShards(host, port, "master");
		// slave链接
		if (SlaveHost != null) {
			SlaveHost = "127.0.0.1";
		}
		SlavePort = SlavePort > 0 ? SlavePort : 6379;
		setShards(SlaveHost, SlavePort, "slave");
		// 构造池
		shardedJedisPool = new ShardedJedisPool(jedisPoolConfig, shards);
		shardedJedis = shardedJedisPool.getResource();
		jedis = shardedJedis.getShard("master");
		return shardedJedisPool;
	}
	
	public static void setShards(String host, int port, String name) {
		shards.add(new JedisShardInfo(host, port, name));
	}
	
}
