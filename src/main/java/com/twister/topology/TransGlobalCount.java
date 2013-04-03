package com.twister.topology;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.coordination.BatchOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.testing.MemoryTransactionalSpout;
import backtype.storm.testing.MemoryTransactionalSpoutMeta;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBatchBolt;
import backtype.storm.topology.base.BaseTransactionalBolt;
import backtype.storm.transactional.ICommitter;
import backtype.storm.transactional.TransactionAttempt;
import backtype.storm.transactional.TransactionalTopologyBuilder;
import backtype.storm.transactional.partitioned.IPartitionedTransactionalSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.RegisteredGlobalState;
import backtype.storm.utils.Utils;

import java.io.Serializable;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.jackrabbit.uuid.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is a basic example of a transactional topology. It keeps a count of the
 * number of tuples seen so far in a database. The source of data and the
 * databases are mocked out as in memory maps for demonstration purposes. This
 * class is defined in depth on the wiki at
 * https://github.com/nathanmarz/storm/wiki/Transactional-topologies
 */
public class TransGlobalCount {
	public static Logger logger = LoggerFactory.getLogger(TransGlobalCount.class);
	public static final int PARTITION_TAKE_PER_BATCH = 3;
	
	public static class TransMeta implements Serializable {
		int index;
		int amt;
		
		public TransMeta() {
			
		}
		
		public TransMeta(int index, int amt) {
			this.index = index;
			this.amt = amt;
		}
		
		@Override
		public String toString() {
			return "index: " + index + "; amt: " + amt;
		}
	}
	
	public static class TransSpout implements IPartitionedTransactionalSpout<TransMeta> {
		public static String TX_FIELD = TransMeta.class.getSimpleName() + "/id";
		
		private String _id;
		private String _finishedPartitionsId;
		private int _takeAmt;
		private Fields _outFields;
		private Map<Integer, List<List<Object>>> _initialPartitions;
		
		public TransSpout(Fields outFields, int takeAmt) {
			_id = RegisteredGlobalState.registerState(DATA);
			
			Map<Integer, Boolean> finished = Collections.synchronizedMap(new HashMap<Integer, Boolean>());
			_finishedPartitionsId = RegisteredGlobalState.registerState(finished);
			System.out.println(_id);
			logger.debug(_finishedPartitionsId + " " + Utils.secureRandomLong());
			_takeAmt = takeAmt;
			_outFields = outFields;
			_initialPartitions = DATA;
		}
		
		public boolean isExhaustedTuples() {
			Map<Integer, Boolean> statuses = getFinishedStatuses();
			for (Integer partition : getQueues().keySet()) {
				if (!statuses.containsKey(partition) || !getFinishedStatuses().get(partition)) {
					return false;
				}
			}
			return true;
		}
		
		class Coordinator implements IPartitionedTransactionalSpout.Coordinator {
			
			@Override
			public int numPartitions() {
				return getQueues().size();
			}
			
			@Override
			public boolean isReady() {
				return true;
			}
			
			@Override
			public void close() {
			}
		}
		
		class Emitter implements IPartitionedTransactionalSpout.Emitter<TransMeta> {
			
			Integer _maxSpoutPending;
			Map<Integer, Integer> _emptyPartitions = new HashMap<Integer, Integer>();
			
			public Emitter(Map conf) {
				Object c = conf.get(Config.TOPOLOGY_MAX_SPOUT_PENDING);
				if (c == null)
					_maxSpoutPending = 1;
				else
					_maxSpoutPending = Utils.getInt(c);
			}
			
			@Override
			public TransMeta emitPartitionBatchNew(TransactionAttempt tx, BatchOutputCollector collector,
					int partition, TransMeta lastPartitionMeta) {
				int index;
				if (lastPartitionMeta == null) {
					index = 0;
				} else {
					index = lastPartitionMeta.index + lastPartitionMeta.amt;
				}
				List<List<Object>> queue = getQueues().get(partition);
				int total = queue.size();
				
				int left = total - index;
				int toTake = Math.min(left, _takeAmt);
				logger.debug("" + total + " " + left + " " + toTake);
				TransMeta ret = new TransMeta(index, toTake);
				emitPartitionBatch(tx, collector, partition, ret);
				if (toTake == 0) {
					int curr = Utils.get(_emptyPartitions, partition, 0) + 1;
					_emptyPartitions.put(partition, curr);
					if (curr > _maxSpoutPending) {
						Map<Integer, Boolean> finishedStatuses = getFinishedStatuses();
						// will be null in remote mode
						if (finishedStatuses != null) {
							finishedStatuses.put(partition, true);
						}
					}
				}
				return ret;
			}
			
			@Override
			public void emitPartitionBatch(TransactionAttempt tx, BatchOutputCollector collector, int partition,
					TransMeta partitionMeta) {
				List<List<Object>> queue = getQueues().get(partition);
				for (int i = partitionMeta.index; i < partitionMeta.index + partitionMeta.amt; i++) {
					List<Object> toEmit = new ArrayList<Object>(queue.get(i));
					toEmit.add(0, tx);
					collector.emit(toEmit);
				}
			}
			
			@Override
			public void close() {
			}
			
		}
		
		@Override
		public IPartitionedTransactionalSpout.Coordinator getCoordinator(Map conf, TopologyContext context) {
			return new Coordinator();
		}
		
		@Override
		public IPartitionedTransactionalSpout.Emitter<TransMeta> getEmitter(Map conf, TopologyContext context) {
			return new Emitter(conf);
		}
		
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			List<String> toDeclare = new ArrayList<String>(_outFields.toList());
			toDeclare.add(0, TX_FIELD);
			declarer.declare(new Fields(toDeclare));
		}
		
		@Override
		public Map<String, Object> getComponentConfiguration() {
			Config conf = new Config();
			conf.registerSerialization(MemoryTransactionalSpoutMeta.class);
			return conf;
		}
		
		public void startup() {
			getFinishedStatuses().clear();
		}
		
		public void cleanup() {
			RegisteredGlobalState.clearState(_id);
			RegisteredGlobalState.clearState(_finishedPartitionsId);
		}
		
		private Map<Integer, List<List<Object>>> getQueues() {
			Map<Integer, List<List<Object>>> ret = (Map<Integer, List<List<Object>>>) RegisteredGlobalState
					.getState(_id);
			if (ret != null)
				return ret;
			else
				return _initialPartitions;
		}
		
		private Map<Integer, Boolean> getFinishedStatuses() {
			return (Map<Integer, Boolean>) RegisteredGlobalState.getState(_finishedPartitionsId);
		}
	}
	
	public static final Map<Integer, List<List<Object>>> DATA = new HashMap<Integer, List<List<Object>>>() {
		{
			put(0, new ArrayList<List<Object>>() {
				{
					add(new Values("cat"));
					add(new Values("dog"));
					add(new Values("chicken"));
					add(new Values("cat"));
					add(new Values("dog"));
					add(new Values("apple"));
				}
			});
			put(1, new ArrayList<List<Object>>() {
				{
					add(new Values("cat"));
					add(new Values("dog"));
					add(new Values("apple"));
					add(new Values("banana"));
				}
			});
			put(2, new ArrayList<List<Object>>() {
				{
					add(new Values("cat"));
					add(new Values("cat"));
					add(new Values("cat"));
					add(new Values("cat"));
					add(new Values("cat"));
					add(new Values("dog"));
					add(new Values("dog"));
					add(new Values("dog"));
					add(new Values("dog"));
				}
			});
		}
	};
	
	public static class Value {
		int count = 0;
		BigInteger txid;
	}
	
	public static Map<String, Value> DATABASE = new HashMap<String, Value>();
	public static final String GLOBAL_COUNT_KEY = "GLOBAL-COUNT";
	
	public static class BatchCount extends BaseBatchBolt {
		Object _id;
		BatchOutputCollector _collector;
		
		int _count = 0;
		
		@Override
		public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector, Object id) {
			_collector = collector;
			_id = id;
		}
		
		@Override
		public void execute(Tuple tuple) {
			_count++;
		}
		
		@Override
		public void finishBatch() {
			_collector.emit(new Values(_id, _count));
		}
		
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "count"));
		}
	}
	
	public static class UpdateGlobalCount extends BaseTransactionalBolt implements ICommitter {
		TransactionAttempt _attempt;
		BatchOutputCollector _collector;
		
		int _sum = 0;
		
		@Override
		public void prepare(Map conf, TopologyContext context, BatchOutputCollector collector,
				TransactionAttempt attempt) {
			_collector = collector;
			_attempt = attempt;
		}
		
		@Override
		public void execute(Tuple tuple) {
			_sum += tuple.getInteger(1);
		}
		
		@Override
		public void finishBatch() {
			Value val = DATABASE.get(GLOBAL_COUNT_KEY);
			Value newval;
			if (val == null || !val.txid.equals(_attempt.getTransactionId())) {
				newval = new Value();
				newval.txid = _attempt.getTransactionId();
				if (val == null) {
					newval.count = _sum;
				} else {
					newval.count = _sum + val.count;
				}
				DATABASE.put(GLOBAL_COUNT_KEY, newval);
			} else {
				newval = val;
			}
			logger.info("_attempt: " + _attempt + " count: " + newval.count);
			_collector.emit(new Values(_attempt, newval.count));
		}
		
		@Override
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("id", "sum"));
		}
	}
	
	public static void main(String[] args) throws Exception {
		TransSpout spout = new TransSpout(new Fields("word"), PARTITION_TAKE_PER_BATCH);
		
		TransactionalTopologyBuilder builder = new TransactionalTopologyBuilder("global-count", "spout", spout, 3);
		builder.setBolt("partial-count", new BatchCount(), 5).noneGrouping("spout");
		builder.setBolt("sum", new UpdateGlobalCount()).globalGrouping("partial-count");
		
		LocalCluster cluster = new LocalCluster();
		
		Config config = new Config();
		config.setDebug(true);
		config.setMaxSpoutPending(3);
		
		cluster.submitTopology("global-count-topology", config, builder.buildTopology());
		
		Thread.sleep(3000);
		cluster.shutdown();
	}
}