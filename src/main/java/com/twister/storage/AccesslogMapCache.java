package com.twister.storage;
import java.util.concurrent.ConcurrentMap;

import com.twister.nio.log.AccessLogAnalysis;

public class AccesslogMapCache extends AbstractCache<AccessLogAnalysis>{
	
    public  int expirationMinutes=20; 
    public AccesslogMapCache() {} 

    public ConcurrentMap<String, AccessLogAnalysis>  makeConcurrentMap(){
    	return this.makeMapCache(AccessLogAnalysis.class, expirationMinutes);
    }    
 
}
