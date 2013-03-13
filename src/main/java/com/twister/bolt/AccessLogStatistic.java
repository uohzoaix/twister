package com.twister.bolt;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

public class AccessLogStatistic extends BaseRichBolt {
	private static final Logger LOGR = LoggerFactory.getLogger(AccessLogStatistic.class);
	private static final long serialVersionUID = 2246728833921545677L;
	Integer taskid;
	String name;
	AccessLogAnalysis logalys;
	OutputCollector collector;
	 
	Fields vfields=new Fields("cnt_pv", "cnt_bytes", "cnt_time", "avg_time", "max_time", "min_time",
			"cnt_error", "a", "b", "c", "d", "e");
	
	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.taskid = context.getThisTaskId();
		// Fields, <String, Values>		 
		 
		LOGR.info(String.format(" AccessLogCounter componentId name :%s,task id :%s ", this.name, this.taskid));
	}
	
	@Override
	public void execute(Tuple input) {
		// this tuple 提取次数
		int count = input.size();
		for (int i = 0; i < input.size(); i++) {
			logalys = (AccessLogAnalysis) input.getValue(i);
			String ikey = logalys.getKey();		 
			LOGR.debug(ikey);
			collector.emit(new Values(logalys));
			LOGR.info(String.format("tuple size %s, AccessLogStatu execute result %s ",count,logalys.toString()));
			  
		}		 
	  System.out.println(count);
		// send ok
		collector.ack(input);		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		ArrayList<String> allfields=new ArrayList<String>();
		allfields.add("ukey");
		allfields.addAll(vfields.toList());
		//ukey=>fields
		LOGR.info(String.format("AccessLogCounter OutputFieldsDeclarer is %s",allfields.toString()));
		declarer.declare(new Fields("AccessLog")); 
	 
	}
	
	@Override
	public void cleanup() {
		System.out.println("==cleanup==");
		 
	}
	
}
