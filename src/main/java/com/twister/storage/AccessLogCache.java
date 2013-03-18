package com.twister.storage;
 
import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;

import com.twister.nio.log.AccessLogAnalysis;
import com.twister.utils.JacksonUtils;

public class AccessLogCache extends AbstractCache<AccessLogAnalysis> { 
	
	public AccessLogCache() {
		super();  //must here
	}
	
	// public static void main(String[] args) {
	// AccessLogCache alc=new AccessLogCache();
	// CacheManager cacheManager = alc.create();
	// Cache cache = cacheManager.getCache("AccessLogCache");
	// String
	// jstr="{\"key\":\"20130303#00:00:00#1#/home\",\"cnt_pv\":1,\"cnt_bytes\":3326,\"cnt_time\":12,\"avg_time\":12.0,\"code\":200,\"cnt_error\":0,\"a\":1,\"b\":0,\"c\":0,\"d\":0,\"e\":0}";
	// AccessLogAnalysis alys=JacksonUtils.jsonToObject(jstr,
	// AccessLogAnalysis.class);
	// System.out.println(alys);
	// //JacksonUtils.jsonToObject(alys.objectToJson(),AccessLogAnalysis.class)
	// }
	
}
