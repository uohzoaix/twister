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
	
 
	
}
