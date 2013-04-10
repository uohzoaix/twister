package com.twister.storage.redis;

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

import com.mongodb.DB;
import com.twister.storage.mongo.MongoManager;
import com.twister.utils.AppsConfig;

/**
 * This abstract bolt class will help publishing the bolt processing into a
 * redis pubsub channel
 * 
 * @author guoqing
 * 
 */

public class JedisManager {
	protected final static Logger logger = LoggerFactory.getLogger(JedisManager.class);
	private final static int DBIndex = 7;
	public static String host = AppsConfig.getInstance().getValue("redis.master.host");
	public static int port = Integer.valueOf(AppsConfig.getInstance().getValue("redis.master.port"));
	public static int timeout = 1 * 60 * 1000; // 1分钟
	public static String SlaveHost = AppsConfig.getInstance().getValue("redis.slave.host");
	public static int SlavePort = Integer.valueOf(AppsConfig.getInstance().getValue("redis.slave.port"));
	// Jedis客户端池配置
	private JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

	private JedisPool jedisPool;
	private Jedis jedis;
	private static JedisManager SingleInstance = null;

	public static JedisManager getInstance() {
		if (SingleInstance == null) {
			SingleInstance = new JedisManager();
		}
		return SingleInstance;
	}

	public JedisManager() {
		build();
	}

	public void build() {
		build(host, port);
	}

	public void build(String host, int port) {
		if (host == null) {
			host = "127.0.0.1";
			port = 6379;
		}
		jedisPool = createJedisPool(host, port);
		jedis = jedisPool.getResource();
	}

	public Jedis getJedis() {
		if (jedis == null) {
			return JedisManager.getInstance().jedis;
		} else {
			return jedis;
		}
	}

	public JedisPool getJedisPool() {
		if (jedisPool == null) {

			return JedisManager.getInstance().jedisPool;
		} else {
			return jedisPool;
		}
	}

	public JedisPool createJedisPool(String host, int port) {
		try {
			// 池基本配置
			jedisPoolConfig.setMaxActive(5000);
			jedisPoolConfig.setMaxIdle(5000);
			jedisPoolConfig.setMaxWait(10000);
			jedisPoolConfig.setTestOnBorrow(true);
			jedisPool = new JedisPool(jedisPoolConfig, host, port, timeout);
			return jedisPool;
		} catch (Exception e) {
			e.printStackTrace();
			return null;
		}

	}

	public static class JedisExpireHelps {
		public final static int DBIndex = JedisManager.DBIndex;
		public static int expire_1S = 1;
		public static int expire_1M = 60;
		public static int expire_3M = 60 * 3;
		public static int expire_5M = 60 * 5;
		public static int expire_10M = 60 * 10;
		public static int expire_30M = 60 * 30;
		public static int expire_1H = 60 * 60;
		public static int expire_12H = 60 * 60 * 12;
		public static int expire_1DAY = 60 * 60 * 24;
		public static int expire_2DAY = 60 * 60 * 24 * 2;
		public static int expire_WEEKY = 60 * 60 * 24 * 7;
	}

	public static void main(String[] args) {
		Jedis jr = JedisManager.getInstance().jedis;
		String key = "mkey";
		jr.set(key, "test redis");
		System.out.println(jr.get(key));
	}
}