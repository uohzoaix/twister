package com.twister.storage.cache;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

import com.twister.storage.linkedhashmap.ConcurrentLinkedHashMap;

/**
 * MomoLRUMap 基于LRU（最近使用)算法,最多存几个capacity
 * 
 */
public class LRUMap<K, V> {
	private ConcurrentMap<K, V> map;
	private ConcurrentMap<K, Long[]> expireMap;
	private int capacity;
	private long defaultExpireSecond = 10 * 60;
	
	public LRUMap(int capacity) {
		this.capacity = capacity;
		this.map = new ConcurrentLinkedHashMap.Builder<K, V>().maximumWeightedCapacity(capacity).build();
		this.expireMap = new ConcurrentLinkedHashMap.Builder<K, Long[]>().maximumWeightedCapacity(capacity).build();
	}
	
	public V get(K k) {
		Long[] expire = expireMap.get(k);
		if (expire != null && expire.length == 2) {
			// If expired: expireSecond*1000 < (now - createTime)
			if (expire[1] * 1000 < (now() - expire[0])) {
				remove(k);
			}
		} else {
			remove(k);
		}
		
		return map.get(k);
	}
	
	public int size() {
		return this.map.size();
	}
	
	/** expire:second */
	public void put(K k, V v, long expireSecond) {
		Long[] expire = new Long[] { now(), expireSecond };
		this.map.put(k, v);
		this.expireMap.put(k, expire);
	}
	
	/** expire:second */
	public void put(K k, V v) {
		Long[] expire = new Long[] { now(), defaultExpireSecond };
		this.map.put(k, v);
		this.expireMap.put(k, expire);
	}
	
	public V remove(K k) {
		this.expireMap.remove(k);
		return this.map.remove(k);
	}
	
	public void clear() {
		this.map.clear();
		this.expireMap.clear();
	}
	
	public boolean containsKey(K k) {
		return get(k) != null;
	}
	
	public Set<K> keySet() {
		return this.map.keySet();
	}
	
	public Set<Map.Entry<K, V>> entrySet() {
		return this.map.entrySet();
	}
	
	private long now() {
		return System.currentTimeMillis();
	}
	
	public int getCapacity() {
		return this.capacity;
	}
	
}