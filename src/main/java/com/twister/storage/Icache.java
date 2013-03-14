package com.twister.storage;

import java.lang.ref.SoftReference;
import java.lang.ref.WeakReference;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
 

public interface Icache<T> {
	/**
	 * 从redis缓存中取出jsonstring 构造对象
	 * 
	 * @param object
	 *            key
	 * @param object
	 *            cls<t>
	 * @return t
	 */
 
	public <T> T restoreObjectFromJson(String key, Class<T> valueType);
	
	/**
	 * 从redis缓存中取出jsonstring 构造对象
	 * 
	 * @param object
	 *            key
	 * @param object
	 *            cls<t>
	 * @return true is store ok
	 */
	public  boolean  storeObjectToJson(String key, T obj);
	
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
	public ConcurrentMap<String, T> makeMapCache(Class<T> valueType,int expirationMinutes);
	
}
