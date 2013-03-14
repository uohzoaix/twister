package com.twister.bolt;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.ConcurrentMap;

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
import com.twister.storage.AccesslogMapCache;

public class AccessLogStatistic extends BaseRichBolt {
	
	private static final Logger LOGR = LoggerFactory.getLogger(AccessLogStatistic.class);
	private static final long serialVersionUID = 2246728833921545687L;
	private Integer taskid;
	private String name;	 
	private OutputCollector collector;
	// <String, AccessLogAnalysis>
	private static AccesslogMapCache amc = new AccesslogMapCache();
	private static ConcurrentMap<String, AccessLogAnalysis> algMap = amc.makeConcurrentMap();
 
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.taskid = context.getThisTaskId();		
		LOGR.info(String.format(" AccessLogStatistic componentId name :%s,task id :%s ", this.name, this.taskid));
	}
	
	@Override
	public void execute(Tuple input) {
		// this tuple 提取次数
		int count = input.size();
		LOGR.info(String.format("tuple size %s",count));
		count=0; //reset
		for (int i = 0; i < input.size(); i++) {
			//pojo,key为试想拼合的字款
			AccessLogAnalysis logalys = (AccessLogAnalysis) input.getValue(i);
			String ukey = logalys.getKey();
			LOGR.debug(ukey);
			AccessLogAnalysis clog=null;
			if (algMap.containsKey(ukey)) {			 
				clog = algMap.get(ukey);
				if (clog!=null && clog.getKey().equals(ukey)){
					logalys=logalys.calculate(clog); //在对象里算
				}
			}
			algMap.put(ukey, logalys); 
			count++;
			LOGR.info(String.format("tuple size %s, AccessLogStatistic calculate  %s ", count, logalys.toString()));
		}
		
		//处理结果发到下一级结点
		count=0; //reset
		//此遍历效率高！
		Iterator<Entry<String, AccessLogAnalysis>> iter=algMap.entrySet().iterator();
		while (iter.hasNext()) {
			Map.Entry<String, AccessLogAnalysis> entry = (Entry<String, AccessLogAnalysis>) iter.next();
			String ukey = entry.getKey();
			//key=20120613#10:01:00#0#01#/home  ?
			String[] tmp=ukey.split("#");
			AccessLogAnalysis logalys = entry.getValue();
			//如果需要可以重组下级key
			logalys.setKey(ukey);
			collector.emit(new Values(logalys));			
			count++;
			LOGR.info(String.format("tuple size %s, AccessLogStatistic result  %s ", count, logalys.toString() ));
	    }
		// send ok
		collector.ack(input);
		// 清空本次处理完的
		algMap.clear();
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) { 
		LOGR.info(String.format("AccessLogStatistic OutputFieldsDeclarer is %s", "AccessLog")) ;
		declarer.declare(new Fields("AccessLog"));		
	}
	
	@Override
	public void cleanup() {
		algMap.clear();		
	}
	
}
