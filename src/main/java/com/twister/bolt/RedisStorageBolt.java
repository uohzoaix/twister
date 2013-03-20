package com.twister.bolt;

import java.math.BigInteger;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
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
import backtype.storm.transactional.ICommitter;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.twister.nio.log.AccessLogAnalysis;
import com.twister.storage.AbstractCache;
import com.twister.storage.AccessLogCache;
import com.twister.utils.JacksonUtils;
import com.twister.utils.JedisConnection.JedisExpireHelps;

/**
 * after statistic analysis accessLog to save redis
 * 
 * @author guoqing
 * 
 */
public class RedisStorageBolt extends BaseRichBolt {
	
	private static final Logger LOGR = LoggerFactory.getLogger(RedisStorageBolt.class);
	private static final long serialVersionUID = 2246728833921545687L;
	private Integer taskid;
	private String name;
	private OutputCollector collector;
	public static int GLOB = 0;
	private CacheManager cacheManager;
	private Cache ehcache;
	private AccessLogCache alc;
	private Map<String, AccessLogAnalysis> hashApiKeys;
	private Map<String, Integer> hashCounter;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.taskid = context.getThisTaskId();
		alc = new AccessLogCache();
		cacheManager = CacheManager.create(getClass().getClassLoader().getResource("conf/ehcache.xml"));
		ehcache = cacheManager.getCache("AccessLogCache");
		hashApiKeys = new ConcurrentHashMap<String, AccessLogAnalysis>();
		hashCounter = new ConcurrentHashMap<String, Integer>();
		LOGR.info(String.format(" RedisStorageBolt componentId name :%s,task id :%s ", this.name, this.taskid));
	}
	
	@Override
	public void execute(Tuple input) {
		// this tuple 提取次数
		
		try {
			String ukey = input.getString(0);
			AccessLogAnalysis logalys = (AccessLogAnalysis) input.getValue(1);
			LOGR.debug(ukey);
			Jedis jedis = alc.getJedisConn().getMasterJedis();
			jedis.select(JedisExpireHelps.DBIndex);
			//String jsonString = jedis.get(ukey);
			//AccessLogAnalysis oldalys = JacksonUtils.jsonToObject(jsonString, AccessLogAnalysis.class);
			//first from cache
			Element element = ehcache.get(ukey);			
			if (element != null) {
				AccessLogAnalysis clog = (AccessLogAnalysis) element.getObjectValue();
				if (clog != null && clog.getKey().equals(ukey)) {
					logalys = logalys.calculate(clog,logalys); // 在对象里算
				}
				Element el2 = new Element(ukey, logalys);
				ehcache.replace(element, el2);
			} else {
				ehcache.put(new Element(ukey, logalys));
			}
		 
			hashCounter.put(ukey, (int) logalys.getCnt_pv());
			hashApiKeys.put(ukey, logalys);
			//save to jedis db
			if (ukey.length() > 0 && logalys != null) {
				String jsonStr = JacksonUtils.objectToJson(logalys);
				jedis.set(ukey, jsonStr);
				jedis.expire(ukey, JedisExpireHelps.expire_2DAY);
			}
			//LOGR.info(String.format("RedisStorageBolt calculate  %s ", logalys.toString()));
			//monitor
			if (ukey.contains("initial")) {
				GLOB++;
				if (hashApiKeys.containsKey(ukey)) {
					int ct3 = hashCounter.get(ukey);
					AccessLogAnalysis alys = (AccessLogAnalysis) hashApiKeys.get(ukey);
					String str3 = jedis.get(ukey);
					AccessLogAnalysis jlg = JacksonUtils.jsonToObject(str3, AccessLogAnalysis.class);
					System.out.println("jedis calculate counter :" + ukey + " cnt_pv= " + ct3 + "  echace ="
							+ alys.getCnt_pv() + " jredis =" + jlg.getCnt_pv());
				}
			}
			
			LOGR.info(String.format("name :%s,task id :%s GLOB %s", this.name, this.taskid, GLOB));
			// 通过ack操作确认这个tuple被成功处理
			collector.ack(input);
			
		} catch (Exception e) {
			LOGR.info(e.getStackTrace().toString());
		}
		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		LOGR.info(String.format("RedisStorageBolt OutputFieldsDeclarer is %s", "AccessLog"));
		declarer.declare(new Fields("ukey", "AccessLogAnalysis"));
	}
	
	@Override
	public void cleanup() {
		hashApiKeys.clear();
		hashCounter.clear();
	}
	
}
