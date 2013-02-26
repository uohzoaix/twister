package com.twister.bolt;

import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

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
public class WordExtractorBolt extends BaseRichBolt {
	OutputCollector collector;

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
	}
	
	@Override
	public void execute(Tuple input) {
		String line = input.getString(0);

		if (line != null) {
			StringTokenizer st = new StringTokenizer(line, " ,.;");
			while (st.hasMoreTokens()) {
				String word = st.nextToken();
				collector.emit(new Values(word));
			}
		}
       // 通过ack操作确认这个tuple被成功处理
		collector.ack(input);
	}
	 
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("word"));
	}

}
