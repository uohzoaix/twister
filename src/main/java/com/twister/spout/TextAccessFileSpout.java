package com.twister.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.IRichSpout;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.common.base.Preconditions;
import com.twister.entity.AccessLog;
import com.twister.utils.Common;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * Spout to feed messages into Storm from a file.
 * <p>
 * This spout emits tuples containing only one field, named "line" for each file
 * line.
 * </p>
 * 
 * <pre></pre>
 * 
 * @author guoqing
 * 
 */
public class TextAccessFileSpout extends BaseRichSpout {
	
	protected final Logger logger = LoggerFactory.getLogger(getClass());
	
	private SpoutOutputCollector collector;
	private RandomAccessFile reader;
	private String filename;
	private File file;
	private boolean open = false;
	private Fields _fields = new Fields("AccessLog");
	public static final Pattern Ipv4 = Common.Ipv4;
	
	public TextAccessFileSpout(String filename) {
		this(new File(filename));
	}
	
	public TextAccessFileSpout(File file) {
		System.out.println(file.isFile());
		Preconditions.checkArgument(file.isFile(), "TextFileSpout expects a file but '" + file + "' is not exists.");
		this.filename = file.getAbsolutePath();
		this.file = file;
	}
	
	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
		try {
			this.collector = collector;
			logger.info("opening TextFileSpout on file " + filename);
			this.reader = new RandomAccessFile(file, "r");
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
		Preconditions.checkState(open && reader != null, "The file " + filename
				+ " must be open before reading from it");
		try {
			AccessLog alog = null;
			String line = reader.readLine();
			if (line == null)
				return;
			if (line != null && line.length() > 12 && Ipv4.matcher(line).find()) {
				alog = new AccessLog(line);
				if (alog != null) {
					collector.emit(new Values(alog));
				}
			}
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}
	
	/**
	 * Emits tuples containing only one field, named "line".
	 */
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(_fields);
	}
	
	public boolean isDistributed() {
		return false;
	}
	
	public void ack(Object msgId) {
		logger.info("TextFileSpout ack ok,msgid " + msgId.toString());
	}
	
	public void fail(Object msgId) {
		logger.info("TextFileSpout fail, msgid " + msgId.toString());
	}
	
}