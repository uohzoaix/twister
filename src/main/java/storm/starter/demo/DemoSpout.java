package storm.starter.demo;
 
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

public class DemoSpout implements IRichSpout {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3980242432526476937L;
	private static Logger log = LoggerFactory.getLogger(DemoSpout.class.getName());
	Queue<Person> queues = new LinkedList<Person>();
	List<Person> list=null;
	
	private SpoutOutputCollector collector;
	private Map conf;
	private TopologyContext context;


	/* (non-Javadoc)
	 * @see backtype.storm.topology.IRichSpout#isDistributed()
	 */


	/* (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#ack(java.lang.Object)
	 */
	@Override
	public void ack(Object arg0) {
		// TODO Auto-generated method stub
		log.debug("ack-----> "+arg0);
	}

	/* (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#close()
	 */
	@Override
	public void close() {
		// TODO Auto-generated method stub
		log.debug("close-----> ");
	}

	/* (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#fail(java.lang.Object)
	 */
	@Override
	public void fail(Object arg0) {
		// TODO Auto-generated method stub
		log.debug("fail-----> "+arg0);
	}

	/* (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#nextTuple()
	 */
	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		Person p= queues.poll();
		if(p!=null){
			log.debug("nextTuple----> 发送数据，用户ID="+p.getId());
			collector.emit(new Values(p), p);
		}
		
	}

	/* (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#open(java.util.Map, backtype.storm.task.TopologyContext, backtype.storm.spout.SpoutOutputCollector)
	 */
	@Override
	public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		log.debug("spout-----> 将集合数据转换至队列中");
		this.collector = collector;
		this.conf = conf;
		this.context = context;
		
		Iterator<Person> it= list.iterator();
		while(it.hasNext()){
			Person p=it.next();
			queues.add(p);
		}
	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#declareOutputFields(backtype.storm.topology.OutputFieldsDeclarer)
	 */
	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		log.debug("declareOutputFields----> 设置输出字段");
		declarer.declare(new Fields("person"));
	}

	public DemoSpout(List<Person> list) {
		super();
		this.list = list;
	}

	/* (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#activate()
	 */
	@Override
	public void activate() {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see backtype.storm.spout.ISpout#deactivate()
	 */
	@Override
	public void deactivate() {
		// TODO Auto-generated method stub
		
	}

	/* (non-Javadoc)
	 * @see backtype.storm.topology.IComponent#getComponentConfiguration()
	 */
	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
