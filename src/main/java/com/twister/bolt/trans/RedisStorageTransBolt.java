package com.twister.bolt.trans;

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

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
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
public class RedisStorageTransBolt extends BaseTransactionalBolt implements ICommitter {
	private static final Logger LOGR = LoggerFactory.getLogger(RedisStorageTransBolt.class);
	private static final long serialVersionUID = 2246728833921545687L;
	private TransactionAttempt id;
	private BatchOutputCollector collector;
	private Integer taskid;
	private String name;
	private CacheManager cacheManager;
	private Cache cache;
	private AccessLogCache alc;
	private Map<String, AccessLogAnalysis> allApiKeys;
	private Map<String, Integer> allCounter;
	
	@Override
	public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, TransactionAttempt id) {
		this.id = id;
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.taskid = context.getThisTaskId();
		cacheManager = CacheManager.create(getClass().getClassLoader().getResource("conf/ehcache.xml"));
		cache = cacheManager.getCache("AccessLogCache");
		alc = new AccessLogCache();
		allApiKeys = new ConcurrentHashMap<String, AccessLogAnalysis>();
		allCounter = new ConcurrentHashMap<String, Integer>();
		LOGR.info(String.format(" RedisStorageTransBolt TransactionAttempt %s componentId name :%s,task id :%s ",
				this.id, this.name, this.taskid));
		
	}
	
	@Override
	public void execute(Tuple input) {
		System.out.println(input.size());
		try {
			String origin = input.getSourceComponent();
			TransactionAttempt tx = (TransactionAttempt) input.getValueByField("txid");
			// pojo,key为试想拼合的字款 ,time也可以分成2
			// ukey=time#rely#server#uriname
			// 20120613#10:01:00#0#/home
			String ukey = input.getStringByField("ukey");
			AccessLogAnalysis logalys = (AccessLogAnalysis) input.getValueByField("AccessLogAnalysis");
			if (logalys != null) {
				logalys.addObject(allApiKeys, ukey, logalys);
				logalys.addCnt(allCounter, ukey);
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOGR.error(e.getStackTrace().toString());
		}
	}
	
	@Override
	public void finishBatch() {
		try {
		} catch (Exception e) {
			e.printStackTrace();
			LOGR.error(e.getStackTrace().toString());
		}
		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		LOGR.info(String.format("RedisStorageBolt OutputFieldsDeclarer is %s", "AccessLog"));
		declarer.declare(new Fields("txid", "ukey", "AccessLogAnalysis"));
		
	}
	
}
