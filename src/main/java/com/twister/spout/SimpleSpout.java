package com.twister.spout;

import java.util.Map;
import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

public class SimpleSpout extends BaseRichSpout {
	static final Logger LOG = LoggerFactory.getLogger(SimpleSpout.class);

	/**
     *
     */
	private static final long serialVersionUID = -6335251364034714629L;
	private SpoutOutputCollector collector;
	private static String[] info = new String[] { "hello", "world", "what",
			"when", "where", "who", "why" };
	Random random = new Random();

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("source"));
	}

	@SuppressWarnings("rawtypes")
	@Override
	public void open(Map conf, TopologyContext context,
			SpoutOutputCollector collector) {
		this.collector = collector;
		LOG.debug("SimpleSpout open====");
	}

	@Override
	public void nextTuple() {
		try {			 
			String msg = info[random.nextInt(info.length)];
			collector.emit(new Values(msg));		
			Utils.sleep(1000);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}

	@Override
	public void ack(Object id) {
		LOG.debug("spout ok");
	}

	@Override
	public void fail(Object id) {
		LOG.debug("spout fail");
	}

}