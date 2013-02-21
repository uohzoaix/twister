package com.twister.utils;

import java.util.concurrent.TimeUnit;

/**
 * Utility class
 */
public class TimeUtils {

    private TimeUtils() {
    	
    }

    public static void sleep(long ms) {
    	TimeUtils.sleep(ms, TimeUnit.MILLISECONDS);
    }


    public static void sleep(long wait, TimeUnit timeUnit) {
        try {
            timeUnit.sleep(wait);
        } catch (InterruptedException e) {
        	 throw new RuntimeException(e);
        }
    }
     
}