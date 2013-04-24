package com.twister.storage;

import java.net.URL;
import java.util.concurrent.atomic.AtomicInteger;


import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;

 
import com.twister.utils.JacksonUtils;

public abstract class AbstractCache<T> {
	
	public final AtomicInteger atomicInteger = new AtomicInteger(0);
	public final String ehcachecfg = "conf/ehcache.xml";
	 

	public AbstractCache() {
	}

	 

	public <T> T restoreObjectFromJson(String key, String jsonStr, final Class<T> valueType) {
		return JacksonUtils.jsonToObject(jsonStr, valueType);
	}

	public String storeObjectToJson(String key, T obj) {

		String jsonStr = JacksonUtils.objectToJson(obj);

		return jsonStr;
	}

	public CacheManager create(String cachecfg) {
		return CacheManager.create(cachecfg);
	}
	
	public CacheManager create() {
		URL url = getClass().getClassLoader().getResource(this.ehcachecfg);
		return CacheManager.create(url);
	}
	
	public Cache getCache(String cacheName) {
		return this.create().getCache(cacheName);
	}
	
}
