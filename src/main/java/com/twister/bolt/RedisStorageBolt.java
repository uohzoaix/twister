package com.twister.bolt;
 
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
 
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;
 

import net.sf.ehcache.Cache;
import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Element;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
 
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt; 
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
 
import com.twister.nio.log.AccessLogAnalysis; 
import com.twister.storage.AbstractCache;
import com.twister.storage.AccessLogCache;
import com.twister.utils.JacksonUtils; 
import com.twister.utils.JedisConnection.JedisExpireHelps;
 
 
/**
 * after   statistic analysis accessLog to save redis 
 * @author guoqing
 * 
 */
public class RedisStorageBolt extends BaseRichBolt {

	
	private static final Logger LOGR = LoggerFactory.getLogger(RedisStorageBolt.class);
	private static final long serialVersionUID = 2246728833921545687L;
	private Integer taskid;
	private String name;
	private OutputCollector collector; 
	private CacheManager cacheManager;
	private Cache cache;
	private AccessLogCache alc;
		
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.taskid = context.getThisTaskId();
		cacheManager = CacheManager.create("src/main/resources/conf/ehcache.xml");
		cache = cacheManager.getCache("AccessLogCache");	
		alc=new AccessLogCache();
		LOGR.info(String.format(" RedisStorageBolt componentId name :%s,task id :%s ", this.name, this.taskid));
	}
	
	@Override
	public void execute(Tuple input) {
		// this tuple 提取次数
	 
		int count = input.size();
		System.out.println(cache.getSize());
		LOGR.info(String.format("tuple size %s", count));
		count = 0; // reset
		try {		 
			String ukey = input.getString(0);
			AccessLogAnalysis logalys = (AccessLogAnalysis) input.getValue(1);
			LOGR.debug(ukey);
			Element element = cache.get(ukey);			
			if (element != null) {
				AccessLogAnalysis clog = (AccessLogAnalysis) element.getObjectValue();
				if (clog != null && clog.getKey().equals(ukey)) {
					logalys = logalys.calculate(clog); // 在对象里算
				}
				Element el2 = new Element(ukey, logalys);
				cache.replace(element, el2);
			} else {
				cache.put(new Element(ukey, logalys));
			}
			if (ukey.length()>0 && logalys!=null){			
				//this.collector.emit(new Values(ukey,logalys));
				String jsonStr=JacksonUtils.objectToJson(logalys);
				if (jsonStr.length()>0){
					Jedis jedis=alc.getJedisConn().getMasterJedis();
					jedis.select(JedisExpireHelps.DBIndex);
					jedis.set(ukey, jsonStr);					
					jedis.expire(ukey,JedisExpireHelps.expire_2DAY);
				}
				LOGR.info(String.format("RedisStorageBolt calculate  %s ", logalys.toString()));
			}
			// 通过ack操作确认这个tuple被成功处理
			collector.ack(input);
			
		} catch (Exception e) {
			e.printStackTrace();
			LOGR.error(e.getStackTrace().toString());
		}
		System.out.println("cache size="+cache.getSize());
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		LOGR.info(String.format("RedisStorageBolt OutputFieldsDeclarer is %s", "AccessLog"));
		declarer.declare(new Fields("ukey", "AccessLogAnalysis"));
	}
	
	@Override
	public void cleanup() {
		
	}
	

}
