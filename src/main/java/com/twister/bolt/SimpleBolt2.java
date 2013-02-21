package com.twister.bolt;

import java.util.HashMap;
import java.util.Map;

 
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.task.TopologyContext;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

public class SimpleBolt2 extends BaseBasicBolt {

    /**
     *
     */
	static final Logger LOG = LoggerFactory.getLogger(SimpleBolt2.class);
    private static final long serialVersionUID = 2246728833921545676L;
    Integer id;
    String name;
    Map<String, Integer> counters;

    @SuppressWarnings("rawtypes")
    public void prepare(Map stormConf, TopologyContext context) {
        this.counters=new HashMap<String, Integer>();
        this.name=context.getThisComponentId();
        this.id=context.getThisTaskId();
        LOG.info(String.format(" bolt2 componentId :%s,task id :%s ",this.name,this.id));        
    }

    public void execute(Tuple input, BasicOutputCollector collector) {
        String word=input.getString(0);
        if(counters.containsKey(word)){
            Integer c=counters.get(word);
            counters.put(word, c+1);
        }
        else{
            counters.put(word, 1);
        }
        collector.emit(new Values(word,counters.get(word)));
        LOG.info(String.format("stats result is:%s:%s", word,counters.get(word)));
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("word","count"));
    }

}