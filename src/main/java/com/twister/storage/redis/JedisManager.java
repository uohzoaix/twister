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

	private JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();
	private List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
	private ShardedJedisPool shardedJedisPool = null;
	private ShardedJedis shardedJedis = null;
	private static JedisManager SingleInstance = null;

	public static JedisManager getInstance() {
		if (SingleInstance == null) {
			SingleInstance = new JedisManager();
		}
		return SingleInstance;
	}

	public JedisManager() {
		createShardedJedis();
	}

	public List<JedisShardInfo> getShards() {
		return shards;
	}

	public ShardedJedisPool getShardedJedisPool() {
		return shardedJedisPool;
	}

	public ShardedJedis getShardedJedis() {
		return shardedJedis;
	}

	public void setShardedJedis(ShardedJedis shardedJedis) {
		this.shardedJedis = shardedJedis;
	}

	public Jedis getJedis() {
		return JedisManager.getInstance().getMasterJedis();
	}
	public Jedis getMasterJedis() {
		Jedis jedis = JedisManager.getInstance().getShardedJedis().getShard("master");
		jedis.select(DBIndex);
		return jedis;
	}
	
	public Jedis getSlaveJedis() {
		Jedis jedis = JedisManager.getInstance().getShardedJedis().getShard("slave");
		return jedis;
	}
	
	public ShardedJedis createShardedJedis() {
		try {
			// 池基本配置
			jedisPoolConfig.setMaxActive(5000);
			jedisPoolConfig.setMaxIdle(5000);
			jedisPoolConfig.setMaxWait(10000);
			jedisPoolConfig.setTestOnBorrow(true);
			if (host == null) {
				host = "127.0.0.1";
				port = 6379;
			}
			JedisPool jedisPool = new JedisPool(jedisPoolConfig, host, port, timeout); // or
																						// pool
			// shard pool
			shards.add(new JedisShardInfo(host, port, timeout, "master"));
			// slave链接
			if (SlaveHost == null) {
				SlaveHost = "127.0.0.1";
				SlavePort = 6379;
			}
			
			shards.add(new JedisShardInfo(SlaveHost, SlavePort, timeout, "slave"));
			// 构造池
			shardedJedisPool = new ShardedJedisPool(jedisPoolConfig, shards);
			shardedJedis = shardedJedisPool.getResource();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			shardedJedisPool.returnResource(shardedJedis);
		}
		logger.info("jedis master " + host + ":" + port);
		logger.info("jedis slave " + SlaveHost + ":" + SlavePort);
		return shardedJedis;
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

}
