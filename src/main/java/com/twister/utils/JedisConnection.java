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

public final class JedisConnection {
	protected final static Logger logger = LoggerFactory.getLogger(JedisConnection.class);	
	public static JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();	 
	public final static int DBIndex=7; 
	
	public static class JedisExpireHelps{
		public final static int DBIndex=JedisConnection.DBIndex; 
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
	
	public static String host = "127.0.0.1";
	public static int port = 6379;
	public static int timeout=1*60*1000;   //1分钟
	public static List<JedisShardInfo> shards = new ArrayList<JedisShardInfo>();
	public static String SlaveHost = AppsConfig.getInstance().getValue("redis.slave.host");
	public static int SlavePort = Integer.valueOf(AppsConfig.getInstance().getValue("redis.slave.port"));
	

	private ShardedJedisPool shardedJedisPool=null;
	private ShardedJedis shardedJedis=null;	
	public JedisConnection() {
		createShardedJedis();
	}
    
	public Jedis getMasterJedis(){		 
		Jedis jedis = shardedJedis.getShard("master");
		jedis.select(DBIndex);
		return jedis;
	}
	
	public Jedis getSlaveJedis(){		 
		Jedis jedis = shardedJedis.getShard("slave");		
		return jedis;
	}
	
	public ShardedJedis createShardedJedis() {		
		try {
			// 池基本配置
			jedisPoolConfig.setMaxActive(5000);
			jedisPoolConfig.setMaxIdle(5000);
			jedisPoolConfig.setMaxWait(10000);
			jedisPoolConfig.setTestOnBorrow(true);		 	      
			host = AppsConfig.getInstance().getValue("redis.master.host");
			port = Integer.valueOf(AppsConfig.getInstance().getValue("redis.master.port"));
			if (host != null) {
				host = "127.0.0.1";
			}
			port = port > 0 ? port : 6379;
			JedisPool jedisPool = new JedisPool(jedisPoolConfig, host, port,timeout);	// or pool
			//shard pool
			shards.add(new JedisShardInfo(host, port,timeout, "master"));
			// slave链接
			if (SlaveHost != null) {
				SlaveHost = "127.0.0.1";
			}
			SlavePort = SlavePort > 0 ? SlavePort : 6379;
			 
			shards.add(new JedisShardInfo(SlaveHost, SlavePort,timeout, "slave"));
			// 构造池
			shardedJedisPool = new ShardedJedisPool(jedisPoolConfig, shards);
			shardedJedis = shardedJedisPool.getResource();			
		} catch (Exception e) {			
			e.printStackTrace();
		}finally {  
			shardedJedisPool.returnResource(shardedJedis);
        }  
		return shardedJedis;
	}
	
 
	
}
