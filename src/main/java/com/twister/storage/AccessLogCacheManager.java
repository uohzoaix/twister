package com.twister.storage;

import redis.clients.jedis.Jedis;
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import com.twister.utils.JacksonUtils;
import com.twister.utils.JedisConnection;
import com.twister.utils.JedisConnection.JedisExpireHelps;
import com.twister.entity.AccessLogAnalysis;
import com.twister.storage.cache.EhcacheMap;

public class AccessLogCacheManager extends AbstractCache<AccessLogAnalysis> {
	
	private static final long serialVersionUID = 1907887314594637890L;
	private EhcacheMap<String, Integer> mapCounter;
	private EhcacheMap<String, AccessLogAnalysis> mapEhcache;
	private Jedis jedis;
	
	public AccessLogCacheManager() {
	}
	
	public synchronized EhcacheMap<String, Integer> getMapCounter() {
		if (mapCounter == null) {
			mapCounter = new EhcacheMap<String, Integer>("EhcacheMap");
		}
		return mapCounter;
	}
	
	public Jedis getMasterJedis() {
		jedis = this.getJedisConn().getMasterJedis();
		return jedis;
	}
	
	public synchronized EhcacheMap<String, AccessLogAnalysis> getMapEhcache() {
		if (mapEhcache == null) {
			mapEhcache = new EhcacheMap<String, AccessLogAnalysis>("AccessLogCache");
		}
		return mapEhcache;
	}
	
}
