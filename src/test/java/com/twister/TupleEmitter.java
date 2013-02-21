package com.twister;


import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.topology.IRichSpout;

import java.util.concurrent.CountDownLatch;

public class TupleEmitter extends Thread {

        private IRichSpout spout;
        private SpoutOutputCollector collector;
        private CountDownLatch done;
        private long numberOfNextCalls;

        public TupleEmitter(final IRichSpout spout, final SpoutOutputCollector collector, final CountDownLatch done, final long numberOfNextCalls) {
            this.spout = spout;
            this.collector = collector;
            this.done = done;
            this.numberOfNextCalls = numberOfNextCalls;
        }

        @Override
        public void run() {
            // Open the spout
            spout.open(null, null, collector);

            // Call next tuple
            for (int i = 0; i < numberOfNextCalls; i++) {
                spout.nextTuple();
            }

            // Close the spout
            spout.close();
            done.countDown();
        }
    }
