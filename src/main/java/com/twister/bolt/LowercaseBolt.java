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

/**
 * @author zhouguoqing
 *
 */
public class LowercaseBolt extends BaseRichBolt {
	OutputCollector collector;
	private static final long serialVersionUID = -5266922733759958473L;
	private static final Logger LOG = LoggerFactory.getLogger(LowercaseBolt.class);

	 
	@Override
	public void prepare(Map stormConf, TopologyContext context,OutputCollector collector) {
		this.collector=collector;
	}

	 
	@Override
	public void execute(Tuple input) {
		LOG.info("LowercaseBolt execute input tuple size "+input.size());
		collector.emit(new Values(input.getString(0).toLowerCase()));
		collector.ack(input);
	}
 
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}