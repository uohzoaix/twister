package com.twister.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.commons.io.input.Tailer;
import org.apache.commons.io.input.TailerListener;
import org.apache.commons.io.input.TailerListenerAdapter;

import java.io.File;
import java.util.Map;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.TimeUnit;

/**
 * Spout to feed messages into Storm from a tailed file.
 * <p>
 * This spout emits tuples containing only one field, named "line" for each file line.
 * </p>
 */
public class TailFileSpout extends BaseRichSpout {

    protected final Logger logger = LoggerFactory.getLogger(getClass());
    public static final int DEFAULT_DELAY = 1000;

    private File file;
    private long interval;
    private volatile boolean stop = false;
    private SpoutOutputCollector collector;
    private SynchronousQueue<String> queue = new SynchronousQueue<String>();
    private Tailer tailer;

    /**
     * Creates a TailFileSpout for the given file, starting from the beginning, with the default interval of 1.0s.
     *
     * @param filename the name of the file to follow.
     */
    public TailFileSpout(String filename) {
        this(filename, DEFAULT_DELAY);
    }

    /**
     * Creates a TailFileSpout for the given file, starting from the beginning.
     *
     * @param filename the name of the file to follow.
     * @param interval    the interval between checks of the file for new content in milliseconds.
     */
    public TailFileSpout(String filename, long interval) {
        this(new File(filename), interval);
    }


    /**
     * Creates a TailFileSpout for the given file, starting from the beginning, with the default interval of 1.0s.
     *
     * @param file the file to follow.
     */
    public TailFileSpout(File file) {
        this(file, DEFAULT_DELAY);
    }

    /**
     * Creates a TailFileSpout for the given file, starting from the beginning.
     *
     * @param file  the file to follow.
     * @param interval the interval between checks of the file for new content in milliseconds.
     */
    public TailFileSpout(File file, long interval) {
        Preconditions.checkArgument(file.isFile(), "TailFileSpout expects a file but '" + file + "' is not.");
        this.file = file;
        this.interval = interval;
    }


    @Override
    public void open(Map map, TopologyContext topologyContext, SpoutOutputCollector collector) {
        this.collector = collector;
        TailerListener listener = new QueueSender(); // This listener send each file line in the queue
        tailer = Tailer.create(file, listener, interval); // Start a tailer thread
        logger.info("Opening TailFileSpout on file " + file.getAbsolutePath() + " with an interval of " + interval + " ms.");
    }

    @Override
    public void nextTuple() {
        try {
            while (!stop) {
                String line = queue.poll(100, TimeUnit.MILLISECONDS); // Wait for a file line from the queue
                logger.debug("Poll a new line from the queue : " + line);
                if (line != null) {
                    collector.emit(new Values(line));
                    return;
                }
            }
        } catch (InterruptedException e) {
            logger.error("Tailing on file " + file.getAbsolutePath() + " was interrupted.");
        }
    }

    @Override
    public void close() {
        stop = true;
        tailer.stop();
        logger.info("Closing TailFileSpout on file " + file.getAbsolutePath());
    }

    /**
     * Emits tuples containing only one field, named "line".
     */
    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

    @Override
    public void ack(Object o) {
    }

    @Override
    public void fail(Object o) {
    }

 
    public boolean isDistributed() {
        return false;
    }

    /**
     * A listener for the tailer sending current file line in a blocking queue.
     */
    private class QueueSender extends TailerListenerAdapter {
        @Override
        public void handle(String line) {
            try {
                logger.debug("Put a new line in the queue : " + line);
                queue.put(line);
            } catch (InterruptedException e) {
                logger.error("Tailing on file " + file.getAbsolutePath() + " was interrupted.");
            }
        }

        @Override
        public void fileRotated() {
            logger.info("File was rotated or rename");
        }
    }

}