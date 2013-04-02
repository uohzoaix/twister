package com.twister.bolt;

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

/**
 * Group直接发给
 * 
 * @author guoqing
 * 
 */
public class AccessLogGroup extends BaseRichBolt {
	private String progname = getClass().getSimpleName();
	private final Logger LOGR = LoggerFactory.getLogger(getClass().getName());
	private static final long serialVersionUID = 2246728833921545687L;
	private Integer taskid;
	private String name;
	private OutputCollector collector;
	private Long GLOB = 0l;
	
	@Override
	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
		this.collector = collector;
		this.name = context.getThisComponentId();
		this.taskid = context.getThisTaskId();
		LOGR.info(String.format(" componentId name :%s,task id :%s ", this.name, this.taskid));
	}
	
	@Override
	public void execute(Tuple input) {
		// this tuple 提取次数
		// GLOB += 1;
		try {
			String ukey = input.getStringByField("ukey");
			// LOGR.info(String.format(GLOB + " %s", ukey));
			AccessLogAnalysis logalys = (AccessLogAnalysis) input.getValueByField("AccessLogAnalysis");
			if (logalys != null) {
				collector.emit(new Values(ukey, logalys));
			}
			// 通过ack操作确认这个tuple被成功处理
			collector.ack(input);
			// LOGR.info("AccessLogGroup==grpRow===" + GLOB);
		} catch (Exception e) {
			e.printStackTrace();
			LOGR.error(e.getStackTrace().toString());
		}
		
	}
	
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("ukey", "AccessLogAnalysis"));
	}
	
	@Override
	public void cleanup() {
	}
	
}
