package com.twister.storage;

import java.net.URL;
import java.util.concurrent.atomic.AtomicInteger;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
 
import com.twister.utils.JacksonUtils;
import com.twister.utils.JedisConnection;
 

public abstract class AbstractCache<T> implements Icache<T>{ 
	
	public final AtomicInteger atomicInteger = new AtomicInteger(0);
	public final String ehcachecfg="conf/ehcache.xml"; 
	private JedisConnection jedisConn=null;
	
	public AbstractCache(){
		jedisConn=new JedisConnection();
	}
	
	public JedisConnection getJedisConn() {
		return jedisConn;
	}

	public void setJedisConn(JedisConnection jedisConn) {
		this.jedisConn = jedisConn;
	}

	
	
	public <T> T restoreObjectFromJson(String key, final Class<T> valueType) {
		Jedis jedis=jedisConn.getMasterJedis();
		if (jedis.exists(key)){
		    String jsonStr = jedis.get(key);
			return JacksonUtils.jsonToObject(jsonStr,valueType);
		}else{			 	
			 
			return null;
		}
	}
	
	public boolean storeObjectToJson(String key, T obj){
		Jedis jedis=jedisConn.getMasterJedis();
		String jsonStr=JacksonUtils.objectToJson(obj);
		if (jsonStr.length()>0){
			jedis.set(key, jsonStr);
			return true;
		}else{
			return false;
		}
		 
	}
	
	public CacheManager create(String cachecfg){		 
		return CacheManager.create(cachecfg);		
	}
	
	public CacheManager create(){
		URL url =getClass().getClassLoader().getResource(this.ehcachecfg);
		return CacheManager.create(url);
	}
	
	public Cache getCache(String cacheName){  
	 return  this.create().getCache(cacheName);
	}
	
}
