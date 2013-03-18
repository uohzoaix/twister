package com.twister.bolt.trans;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.TimeUnit;

import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.twister.nio.log.AccessLog;
import com.twister.nio.log.AccessLogAnalysis;

public class AccessLogTransStatis extends BaseBatchBolt {
	
	private static final Logger LOGR = LoggerFactory.getLogger(AccessLogTransStatis.class);
	private static final long serialVersionUID = 2246728833921545687L;
	private Integer taskid;
	private String name;
	private Object id;
	private Map<String, AccessLogAnalysis> hashApiKeys;
	private Map<String, Integer> hashCounter;
	private BatchOutputCollector collector;
	// <String, AccessLogAnalysis>
	private CacheManager cacheManager;
	private Cache cache;
	
	@Override
	public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.taskid = context.getThisTaskId();
		this.id = id;
		hashApiKeys = new ConcurrentHashMap<String, AccessLogAnalysis>();
		hashCounter = new ConcurrentHashMap<String, Integer>();
		cacheManager = CacheManager.create("src/main/resources/conf/ehcache.xml");
		cache = cacheManager.getCache("AccessLogCache");
		LOGR.info(String.format(" AccessLogStatistic componentId name :%s,task id :%s ", this.name, this.taskid));
		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		LOGR.info(String.format("AccessLogtransStatistic OutputFieldsDeclarer is %s", "AccessLog"));
		declarer.declare(new Fields("txid", "ukey", "AccessLogAnalysis"));
	}
	
	@Override
	public void execute(Tuple input) {
		// this tuple 提取次数
		int count = input.size();
		System.out.println(cache.getSize());
		LOGR.info(String.format("tuple size %s", count));
		count = 0; // reset
		try {
			String source = input.getSourceStreamId();
			TransactionAttempt tx = (TransactionAttempt) input.getValueByField("txid");
			// pojo,key为试想拼合的字款 ,time也可以分成2
			// ukey=time#rely#server#uriname
			// 20120613#10:01:00#0#/home
			String ukey = input.getStringByField("ukey");
			AccessLogAnalysis logalys = (AccessLogAnalysis) input.getValueByField("AccessLogAnalysis");
			if (logalys != null) {
				logalys.addObject(hashApiKeys, ukey, logalys);
				logalys.addCnt(hashCounter, ukey);
			}
		} catch (Exception e) {
			e.printStackTrace();
			LOGR.error(e.getStackTrace().toString());
		}
		System.out.println(cache.getKeys().toString());
	}
	
	@Override
	public void finishBatch() {
		Iterator<Entry<String, AccessLogAnalysis>> iter = hashApiKeys.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<String, AccessLogAnalysis> entry = (Map.Entry<String, AccessLogAnalysis>) iter.next();
			String ukey = (String) entry.getKey();
			AccessLogAnalysis alys = (AccessLogAnalysis) entry.getValue();
			collector.emit(new Values(id, ukey, alys));
			int ct = hashCounter.containsKey(ukey) ? hashCounter.get(ukey) : 1;
			LOGR.info("counter :" + ukey + " " + ct + " " + alys);
		}
	}
	
	
	
}
