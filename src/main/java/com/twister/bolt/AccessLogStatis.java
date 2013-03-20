package com.twister.bolt;

 
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentHashMap;
 

import net.sf.ehcache.CacheManager;
import net.sf.ehcache.Cache;
import net.sf.ehcache.Element;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.twister.nio.log.AccessLogAnalysis;
/**
 * 直接发给redisbolt汇总
 * @author guoqing
 *
 */
public class AccessLogStatis extends BaseRichBolt {
	
	private static final Logger LOGR = LoggerFactory.getLogger(AccessLogStatis.class);
	private static final long serialVersionUID = 2246728833921545687L;
	private Integer taskid;
	private String name;
	private OutputCollector collector;
	// <String, AccessLogAnalysis>
	private Map<String, AccessLogAnalysis> hashApiKeys;
	private Map<String, Integer> hashCounter;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.taskid = context.getThisTaskId();
		hashApiKeys = new ConcurrentHashMap<String, AccessLogAnalysis>();
		hashCounter=new ConcurrentHashMap<String, Integer>();
		LOGR.info(String.format(" AccessLogStatis componentId name :%s,task id :%s ", this.name, this.taskid));
	}
	
	@Override
	public void execute(Tuple input) {
		// this tuple 提取次数
		int count = input.size();		
		LOGR.info(String.format("tuple size %s", count));
		count = 0; // reset
		try {
			// pojo,key为试想拼合的字款 ,time也可以分成2
			// ukey=time#rely#server#uriname
			// 20120613#10:01:00#0#/home
			String ukey = input.getString(0);
			AccessLogAnalysis logalys = (AccessLogAnalysis) input.getValue(1);	
			if (logalys!=null){
				AccessLogAnalysis clog = hashApiKeys.get(ukey);
				if (clog == null) {
					hashApiKeys.put(ukey, logalys);
				} else {
					logalys = logalys.calculate(logalys,clog);
					hashApiKeys.put(ukey, logalys);
				}		
				Integer ct = hashCounter.get(ukey);
				if (ct == null) {
					hashCounter.put(ukey, 1);
				} else {
					ct += 1;
					hashCounter.put(ukey, ct);
				}
				 
			}
			
			Iterator<Entry<String, AccessLogAnalysis>> iter = hashApiKeys.entrySet().iterator();
			while (iter.hasNext()) {
				Map.Entry<String, AccessLogAnalysis> entry = (Map.Entry<String, AccessLogAnalysis>) iter.next();
				String key1 = (String) entry.getKey();
				AccessLogAnalysis alys = (AccessLogAnalysis) entry.getValue();
				collector.emit(new Values(key1, alys));
				int ct = hashCounter.containsKey(ukey) ? hashCounter.get(ukey) : 1;
				LOGR.info("AccessLogStatis calculate counter :" + ukey + " " + ct + " " + alys.toString());
				//clean send ok
				hashCounter.remove(ukey);
				iter.remove();
			}
			// 通过ack操作确认这个tuple被成功处理
			collector.ack(input);
			
		} catch (Exception e) {
			e.printStackTrace();
			LOGR.error(e.getStackTrace().toString());
		}
		 
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		LOGR.info(String.format("AccessLogStatis OutputFieldsDeclarer is %s", "AccessLog"));
		declarer.declare(new Fields("ukey", "AccessLogAnalysis"));
	}
	
	@Override
	public void cleanup() {
		hashApiKeys.clear();
		hashCounter.clear();
	}
 
	 
}
