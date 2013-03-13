package com.twister.storage;

import java.util.Comparator;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.collections.keyvalue.TiedMapEntry;

import com.google.common.collect.MapMaker;
import com.google.common.base.Function;

public class ConcurrentMapCache {
	
	public static void main(String[] args) {
		final AtomicInteger atoid = new AtomicInteger(0);
		ConcurrentMap<String, Object> mapCache = new MapMaker().concurrencyLevel(32).softKeys().weakValues()
				.expiration(3, TimeUnit.SECONDS).makeComputingMap(new Function<String, Object>() {
					
					public Object apply(String key) {
						// Object
						System.out.println("create obj :" + key);
						return atoid.getAndIncrement();
						
					}
					
				});
		mapCache.put("a", "testa");
		mapCache.put("b", "testb");
		System.out.println(mapCache.get("a"));
		System.out.println(mapCache.get("b"));
		System.out.println(mapCache.get("c"));
		 
	 
		Map cm=new ConcurrentHashMap();
		TiedMapEntry te=new TiedMapEntry(cm, "aaaaaaaaa");
		System.out.println("======== "+te.getKey());
		System.out.println(te.getValue());
		te.setValue("444");
		cm.put("bb", 1);
	   
		System.out.println("======== "+te+ cm.values());
	}
	
	public static Comparator<Object> arbitraryOrder() {
		return new Comparator<Object>() {
			private Map<Object, Integer> uids;			
			public int compare(Object left, Object right) {
				if (left == right)
					return 0;
				int leftCode = System.identityHashCode(left);
				int rightCode = System.identityHashCode(right);
				if (leftCode != rightCode)
					return leftCode < rightCode ? -1 : 1;
				synchronized (this) {
					if (uids == null) {
						final AtomicInteger counter = new AtomicInteger(0);
						uids = new MapMaker().weakKeys().makeComputingMap(new Function<Object, Integer>() {
							public Integer apply(Object from) {
								return counter.getAndIncrement();
							}
						});
					}
				}
				return uids.get(left).compareTo(uids.get(right));
				
			};
		};
	}
}
