package com.twister.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.common.base.Preconditions;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;

/**
 * Spout to feed messages into Storm from a file.
 * <p>
 * This spout emits tuples containing only one field, named "line" for each file line.
 * </p>
 */
public class TextFileSpout extends BaseRichSpout {

    protected final Logger logger = LoggerFactory.getLogger(getClass());

    private SpoutOutputCollector collector;
    private RandomAccessFile reader;
    private String filename;
    private File file;
    private boolean open = false;


    public TextFileSpout(String filename) {
        this(new File(filename));
    }

    public TextFileSpout(File file) {
        Preconditions.checkArgument(file.isFile(), "TextFileSpout expects a file but '" + file + "' is not.");
        this.filename = file.getAbsolutePath();
        this.file = file;
    }

    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        try {
            this.collector = collector;
            this.reader = new RandomAccessFile(file, "r");
            logger.info("Opening TextFileSpout on file " + filename);
            open = true;
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }
    }

    public void close() {
        IOUtils.closeQuietly(reader);
        open = false;
        logger.info("Closing TextFileSpout on file " + filename);
    }

    public void nextTuple() {
        Preconditions.checkState(open && reader != null, "The file " + filename + " must be open before reading from it");
        try {
            String line = reader.readLine();
            if (line == null) return;
            collector.emit(new Values(line));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    /**
     * Emits tuples containing only one field, named "line".
     */
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("line"));
    }

    public boolean isDistributed() {
        return false;
    }

    public void ack(Object msgId) {
    }

    public void fail(Object msgId) {
    }


}