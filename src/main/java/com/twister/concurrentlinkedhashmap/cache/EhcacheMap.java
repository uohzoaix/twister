/*
 * Copyright 2011 Google Inc. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.twister.concurrentlinkedhashmap.cache;

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Ehcache;

import net.sf.ehcache.Element;
import net.sf.ehcache.config.CacheConfiguration;
import net.sf.ehcache.store.MemoryStoreEvictionPolicy;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;

/**
 * A typesafe enumeration of eviction policies. The policy used to evict
 * elements from the {@link net.sf.ehcache.store.MemoryStore}. This can be one
 * of:
 * <ol>
 * <li>LRU - least recently used
 * <li>LFU - least frequently used
 * <li>FIFO - first in first out, the oldest element by creation time
 * </ol>
 * EhcacheMap<String, Object> map = new EhcacheMap<String,
 * Object>("EhcacheMap");
 * 
 * @author guoqing
 */
public final class EhcacheMap<K, V> extends AbstractMap<K, V> implements ConcurrentMap<K, V> {
	private final Ehcache map;
	public static final String DEFAULT_CACHE_NAME = "EhcacheMap";
	private CacheManager cacheManager;
	
	public EhcacheMap(String cacheName) {
		cacheManager = CacheManager.create(getClass().getClassLoader().getResource("conf/ehcache.xml"));
		map = cacheManager.getCache(cacheName);
	}
	
	public Ehcache cacheMap() {
		return this.map;
	}
	
	@Override
	public void clear() {
		map.removeAll();
	}
	
	@Override
	public int size() {
		return keySet().size();
	}
	
	@Override
	public boolean containsKey(Object key) {
		return map.isKeyInCache(key);
	}
	
	@Override
	public boolean containsValue(Object value) {
		return map.isValueInCache(value);
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public V get(Object key) {
		Element element = map.get(key);
		return (element == null) ? null : (V) element.getObjectValue();
	}
	
	public Map<K, V> getAll(Collection<? extends K> keys) {
		Map<K, V> results = new HashMap<K, V>(keys.size());
		for (K key : keys) {
			V value = get(key);
			if (value != null) {
				results.put(key, value);
			}
		}
		return results;
	}
	
	@Override
	public V put(K key, V value) {
		V old = get(key);
		map.put(new Element(key, value));
		return old;
	}
	
	@Override
	public V putIfAbsent(K key, V value) {
		V old = get(key);
		if (old == null) {
			map.put(new Element(key, value));
		}
		return old;
	}
	
	@Override
	public V remove(Object key) {
		V old = get(key);
		if (old != null) {
			map.remove(key);
		}
		return old;
	}
	
	@Override
	public boolean remove(Object key, Object value) {
		if (value.equals(get(key))) {
			map.remove(key);
			return true;
		}
		return false;
	}
	
	@Override
	public V replace(K key, V value) {
		V old = get(key);
		if (old != null) {
			map.put(new Element(key, value));
		}
		return old;
	}
	
	@Override
	public boolean replace(K key, V oldValue, V newValue) {
		if (oldValue.equals(get(key))) {
			map.put(new Element(key, newValue));
			return true;
		}
		return false;
	}
	
	@Override
	@SuppressWarnings("unchecked")
	public Set<K> keySet() {
		return new KeySetAdapter<K>(map.getKeys());
	}
	
	@Override
	public Set<Entry<K, V>> entrySet() {
		return getAll(keySet()).entrySet();
	}
	
	/**
	 * Represents the list of keys as a set, which is guaranteed to be true by
	 * {@link Ehcache#getKeys()}'s contract.
	 */
	private static final class KeySetAdapter<K> implements Set<K> {
		private final List<K> keys;
		
		public KeySetAdapter(List<K> keys) {
			this.keys = keys;
		}
		
		@Override
		public boolean add(K o) {
			return keys.add(o);
		}
		
		@Override
		public boolean addAll(Collection<? extends K> c) {
			return keys.addAll(c);
		}
		
		@Override
		public void clear() {
			keys.clear();
		}
		
		@Override
		public boolean contains(Object o) {
			return keys.contains(o);
		}
		
		@Override
		public boolean containsAll(Collection<?> c) {
			return keys.containsAll(c);
		}
		
		@Override
		public boolean isEmpty() {
			return keys.isEmpty();
		}
		
		@Override
		public Iterator<K> iterator() {
			return keys.iterator();
		}
		
		@Override
		public boolean remove(Object o) {
			return keys.remove(o);
		}
		
		@Override
		public boolean removeAll(Collection<?> c) {
			return keys.removeAll(c);
		}
		
		@Override
		public boolean retainAll(Collection<?> c) {
			return keys.retainAll(c);
		}
		
		@Override
		public int size() {
			return keys.size();
		}
		
		@Override
		public boolean equals(Object o) {
			return keys.equals(o);
		}
		
		@Override
		public int hashCode() {
			return keys.hashCode();
		}
		
		@Override
		public Object[] toArray() {
			return keys.toArray();
		}
		
		@Override
		public <T> T[] toArray(T[] a) {
			return keys.toArray(a);
		}
	}
}
