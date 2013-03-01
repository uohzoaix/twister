package storm.starter.demo;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.twister.topology.TwisterTopology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.utils.Utils;


/**
 * TODO
 * 
 * @author cuiran
 * @version TODO
 */
public class DemoTopology {

	private static Logger log = LoggerFactory.getLogger(DemoTopology.class);	

	public static List<Person> getPerson() {
		List<Person> list = new ArrayList<Person>();
		for (int i = 1; i < 10; i++) {
			Person p = new Person();
			p.setId((long) i);
			p.setName("zhang bing" + i);
			p.setAge(20 + i);
			list.add(p);
		}
		return list;
	}

	public static List<String> getStr() {
		List<String> list = new ArrayList<String>();
		for (int i = 1; i < 10; i++) {
			list.add("test" + i);
		}
		return list;
	}

	public static void main(String[] args) throws Exception { 		 
		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("1", new DemoSpout(getPerson()));

		builder.setBolt("2", new ListBolt()).globalGrouping("1");
		// config
		Config conf = new Config();
		conf.setDebug(false);

		if (null != args && args.length > 0) {
			// 使用集群模式运行
			conf.setNumWorkers(3);
			StormSubmitter.submitTopology(args[0], conf,builder.createTopology());
		} else {
			// 使用本地模式运行
			conf.setMaxTaskParallelism(3);
			LocalCluster cluster = new LocalCluster();		 
			cluster.submitTopology("demoPerson", conf,builder.createTopology());			 
			Thread.sleep(10 * 1000);
			cluster.shutdown();

		}
	}

	
}
