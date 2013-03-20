package com.twister.topology;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.StringTokenizer;
import java.util.Vector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twister.spout.TailFileSpout;
import com.twister.spout.TextFileSpout;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

/**
 * <p>
 * Description : WordCountTopology <br>
 * usage: Topology
 * </p>
 * 
 * <pre></pre>
 * 
 * @author guoqing
 * @see TopologyBuilder 
 * 
 */

public class WordCountTopology {
	public static Logger LOGR = LoggerFactory.getLogger(WordCountTopology.class);
	
	public static class LowercaseBolt extends BaseRichBolt {
		OutputCollector collector;
		private static final long serialVersionUID = -5266922733759958473L;
	 
		 
		@Override
		public void prepare(Map stormConf, TopologyContext context,OutputCollector collector) {
			this.collector=collector;
		}
		 
		@Override
		public void execute(Tuple input) {		
			collector.emit(new Values(input.getString(0).toLowerCase()));
			collector.ack(input);
		}
	 
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}

	}
	
	public static class WordExtractorBolt extends BaseRichBolt {		
		OutputCollector collector;		
		@Override
		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
			this.collector = collector;
		}
		
		@Override
		public void execute(Tuple input) {
			String line = (String) input.getValue(0);
			
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
	
	public static class WordCountBolt extends BaseRichBolt {	 
		private static final long serialVersionUID = 2246728833921545675L;
		Integer taskid;
		String name;
		
		OutputCollector collector;
		Map<String, Integer> wordCountMap;
		
		@SuppressWarnings("rawtypes")
		@Override
		public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
			this.collector = collector;
			this.name = context.getThisComponentId();
			this.taskid = context.getThisTaskId();
			this.wordCountMap = new HashMap<String, Integer>();
			LOGR.info(String.format(" WordCountBolt componentId name :%s,task id :%s ", this.name, this.taskid));
		}
		
		@Override
		public void execute(Tuple input) {
			// 提取单词出现次数
			String word = input.getString(0);
			
			LOGR.info(word);
			int count = 0;
			if (wordCountMap.containsKey(word)) {
				count = wordCountMap.get(word).intValue();
			}
			
			// 更新单词出现次数
			count += 1;
			wordCountMap.put(word, count);
			
			// 发射统计结果
			collector.emit(new Values(word, wordCountMap.get(word)));
			LOGR.info(String.format("WordCountBolt execute result is:%s=%s ", word, wordCountMap.get(word)));
			// send ok
			collector.ack(input);
		}
		
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word", "count"));
		}
		
		@Override
		public void cleanup() {
			wordCountMap.clear();
		}
		
	}
	
	public static void main(String[] args) throws Exception {
		String progname="wordcount";
		String filename="words.txt";
		if (args != null && args.length > 0) {
			progname=args[0];
			filename=args[1];			
		}
		System.out.println(""+progname+" "+filename);
		TopologyBuilder builder = new TopologyBuilder();
		TextFileSpout textSpout = new TextFileSpout("/home/guoqing/workspace/twister/target/classes/words.txt");
				
		builder.setSpout("spouter", textSpout);
		builder.setBolt("extract", new WordExtractorBolt(), 3).shuffleGrouping("spouter");
		builder.setBolt("lower", new LowercaseBolt(), 3).shuffleGrouping("extract");
		builder.setBolt("count", new WordCountBolt(), 3).fieldsGrouping("lower", new Fields("word"));
		Config conf = new Config();
		conf.setDebug(true);

		if (args != null && args.length > 0) {
			// 使用集群模式运行
			conf.setNumWorkers(5);
			StormSubmitter.submitTopology(progname, conf,builder.createTopology());
		} else {
			// 使用本地模式运行
			conf.setMaxTaskParallelism(3);
			LocalCluster localCluster = new LocalCluster();
			localCluster.submitTopology(progname, conf,builder.createTopology());
			Thread.sleep(10 * 1000);
			localCluster.shutdown();
		}
	}
}
