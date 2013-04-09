package com.twister.storage;

import com.twister.entity.AccessLogAnalysis;
import com.twister.storage.cache.EhcacheMap;

public class AccessLogCacheManager extends AbstractCache<AccessLogAnalysis> {
	
	private static final long serialVersionUID = 1907887314594637890L;
	private EhcacheMap<String, Integer> mapCounter;
	private EhcacheMap<String, AccessLogAnalysis> mapEhcache;
	
	public AccessLogCacheManager() {
	}
	
	public synchronized EhcacheMap<String, Integer> getMapCounter() {
		if (mapCounter == null) {
			mapCounter = new EhcacheMap<String, Integer>("EhcacheMap");
		}
		return mapCounter;
	}
	
	

	public synchronized EhcacheMap<String, AccessLogAnalysis> getMapEhcache() {
		if (mapEhcache == null) {
			mapEhcache = new EhcacheMap<String, AccessLogAnalysis>("AccessLogCache");
		}
		return mapEhcache;
	}

	
}
