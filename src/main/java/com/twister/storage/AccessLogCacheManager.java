package com.twister.storage;

import redis.clients.jedis.Jedis;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;

import com.twister.concurrentlinkedhashmap.cache.EhcacheMap;
import com.twister.nio.log.AccessLogAnalysis;
import com.twister.utils.JacksonUtils;
import com.twister.utils.JedisConnection;
import com.twister.utils.JedisConnection.JedisExpireHelps;

public class AccessLogCacheManager extends AbstractCache<AccessLogAnalysis> {
	
	private EhcacheMap<String, Integer> mapCounter;
	private EhcacheMap<String, AccessLogAnalysis> mapEhcache;
	private Jedis jedis;
	
	public AccessLogCacheManager() {
	}
	
	public EhcacheMap<String, Integer> getMapCounter() {
		if (mapCounter == null) {
			mapCounter = new EhcacheMap<String, Integer>("EhcacheMap");
		}
		return mapCounter;
	}
	
	public Jedis getMasterJedis() {
		Jedis jedis = this.getJedisConn().getMasterJedis();
		return jedis;
	}
	
	public EhcacheMap<String, AccessLogAnalysis> getMapEhcache() {
		if (mapEhcache == null) {
			mapEhcache = new EhcacheMap<String, AccessLogAnalysis>("AccessLogCache");
		}
		return mapEhcache;
	}
	
}
