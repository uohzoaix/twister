package com.twister.storage;

import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.ShardedJedis;
import redis.clients.jedis.ShardedJedisPool;
 
import com.google.common.collect.MapMaker;
import com.twister.utils.JacksonUtils;
import com.twister.utils.RedisUtils;

public abstract class AbstractCache<T> implements Icache<T>{
	
	private static Logger logger = LoggerFactory.getLogger(AbstractCache.class);
	
	public final AtomicInteger atomicInteger = new AtomicInteger(0);
	
	public static ShardedJedisPool getShardedPool(){
		return RedisUtils.getShardedJedisPool();
	}
	
	public static ShardedJedis getShardedJedis() {		 
		return getShardedPool().getResource();
	}
	
	public static Jedis getMasterJedis() {	
		return getShardedJedis().getShard("master");
	}
	
	public static Jedis getSlaveJedis() {	
		return getShardedJedis().getShard("slave");
	}
	
	public <T> T restoreObjectFromJson(String key, final Class<T> valueType) {
		Jedis jedis=getMasterJedis();
		if (jedis.exists(key)){
		    String jsonStr = jedis.get(key);
			return JacksonUtils.jsonToObject(jsonStr,valueType);
		}else{			 	
			logger.debug("restoreObjectFromJson key "+key +" is not exists redis.");
			return null;
		}
	}
	
	public boolean storeObjectToJson(String key, T obj){
		Jedis jedis=getMasterJedis();
		String jsonStr=JacksonUtils.objectToJson(obj);
		if (jsonStr.length()>0){
			jedis.set(key, jsonStr);
			return true;
		}else{
			return false;
		}
		 
	}
	
	/**
	 *
	 * see A {@link ConcurrentMap} builder, providing any combination of these
	 * features: {@linkplain SoftReference soft} or {@linkplain WeakReference
	 * weak} keys, soft or weak values, timed expiration, and on-demand
	 * computation of values. Usage example: <pre> {@code
	 *
	 *   ConcurrentMap<Key, Graph> graphs = new MapMaker()
	 *       .concurrencyLevel(32)
	 *       .softKeys()
	 *       .weakValues()
	 *       .expiration(30, TimeUnit.MINUTES)
	 *       .makeComputingMap(
	 *           new Function<Key, Graph>() {
	 *             public Graph apply(Key key) {
	 *               return createExpensiveGraph(key);
	 *             }
	 *           });}</pre>
	 *
	 * These features are all optional; {@code new MapMaker().makeMap()}
	 * returns a valid concurrent map that behaves exactly like a
	 * {@link ConcurrentHashMap}.
	 */
 
	@Override
	public ConcurrentMap<String, T> makeMapCache(Class<T> valueType ,int expirationMinutes) {
		/** 
         * softKeys 
         * weakValues 
         * 可以设置key跟value的strong，soft，weak属性。
         * expiration(3, TimeUnit.MINUTES)设置超时时间为3分 
         *  
         */ 
		// simple class?
        //if (valueType instanceof Class<?>) {
        //   Class<?> cls = (Class<?>) valueType;
		//}
		ConcurrentMap<String, T> mapCache = new MapMaker().concurrencyLevel(32)		          
					.expiration(expirationMinutes, TimeUnit.MINUTES)
					.makeMap();
		return 	 mapCache;
	}	
	
}
