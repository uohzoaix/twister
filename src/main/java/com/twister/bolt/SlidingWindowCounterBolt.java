package com.twister.bolt;

import java.util.HashMap;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext; 
import backtype.storm.topology.OutputFieldsDeclarer; 
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;

/**
 * @author guoqing 
 * @version 0.1
 * 
 * <p>
 * Description :
 *  SlidingWindowCounterBolt
 * <br>  
 * usage:
 *  Topology
 * </p>
 *
 */

public class SlidingWindowCounterBolt extends BaseRichBolt {
 
	private static final long serialVersionUID = 1L;

	private OutputCollector collector;

	private int bucketNum = 5;
	private int bucketSize = 60;

	private long startTime;
	private boolean isStarting = true;

	private Map<Object, Integer[]> counterMap = new HashMap<Object, Integer[]>();

	/**
	 * @param bucketNum
	 * @param bucketSize
	 */
	public SlidingWindowCounterBolt(int bucketNum, int bucketSize) {
		this.bucketNum = bucketNum;
		this.bucketSize = bucketSize;
	}

	public void increaseObjectCount(Object object, int count) {
		int index = getBucketIndex(System.currentTimeMillis());

		synchronized (counterMap) {
			Integer[] counts = counterMap.get(object);
			if (counts == null) {
				counts = new Integer[bucketNum];
				counterMap.put(object, counts);
			}
			if (counts[index] == null) {
				counts[index] = count;
			} else {
				counts[index] += count;
			}
		}
	}

	public int getBucketIndex(long milliSecond) {
		return (int) ((milliSecond / 1000) % bucketNum);
	}

	public boolean isStarting() {
		if (isStarting
				&& (System.currentTimeMillis() - startTime) / 1000 >= bucketNum
						* bucketSize) {
			isStarting = false;
		}
		return isStarting;
	}

	public int getTotalCount(Integer[] counts) {
		int sum = 0;
		for (Integer count : counts) {
			sum += count == null ? 0 : count.intValue();
		}
		return sum;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("timestamp", "object", "count"));
	}

	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector _collector) {
		this.collector = _collector;

		startTime = System.currentTimeMillis();

		Thread bucketCleanerThread = new Thread(new Runnable() {

			@Override
			public void run() {
				int lastBucketIndex = getBucketIndex(System.currentTimeMillis());

				while (true) {
					int bucketIndex = getBucketIndex(System.currentTimeMillis());

					if (bucketIndex != lastBucketIndex) {
						synchronized (counterMap) {
							for (Object object : counterMap.keySet()) {
								Integer[] counts = counterMap.get(object);

								int totalCount = getTotalCount(counts);
								collector.emit(new Values(System
										.currentTimeMillis(),
										object.toString(), totalCount));
								System.out.println("!!!!!!!!!!!!!!!Total "
										+ object.toString() + " " + totalCount);

								if (!isStarting()) {
									int bucketToCleanIndex = (bucketIndex + 1)
											% bucketNum;
									counts[bucketToCleanIndex] = 0;
								}
							}
						}

						lastBucketIndex = bucketIndex;
					}

					Utils.sleep((bucketSize - (System.currentTimeMillis() / 1000)
							% bucketSize) * 1000);
				}
			}

		});

		bucketCleanerThread.start();
	}

	@Override
	public void execute(Tuple input) {
		String word = input.getString(0);
		int count = input.getInteger(1);

		System.out.println("!!!!!!!!!!!!!!!Increase " + word + " " + count);
		this.increaseObjectCount(word, count);
		collector.ack(input);
	}
}
