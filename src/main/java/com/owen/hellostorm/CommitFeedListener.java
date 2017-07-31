package com.owen.hellostorm;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;

import org.apache.storm.shade.org.apache.commons.io.IOUtils;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class CommitFeedListener implements IRichSpout
{
	private static final long serialVersionUID = 1L;
	
	// collector is used to emit tuples
	private SpoutOutputCollector collector;
	private List<String> commits; // string messages read from .txt
	
	public void ack(Object arg0){}
	public void activate(){}
	public void close(){}
	public void deactivate(){}
	public void fail(Object arg0){}

	// called by Storm when it's ready to read the next tuple
	public void nextTuple()
	{
		for(String commit : commits)
		{
			// name of commit string is "Commit", defined in declareOutputFields()
			collector.emit(new Values(commit));
		}
	}

	// get called when Storm prepares the spout to be run
	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2)
	{
		this.collector = arg2;
		try
		{
			// read data from .txt with format "10001 15108391078@163.com" each line
			commits = IOUtils.readLines(ClassLoader.getSystemResourceAsStream("Owen.txt"),
					Charset.defaultCharset());
		}
		catch(IOException e)
		{
			throw new RuntimeException(e);
		}
	}

	// define the field names for all the tuples emitted by this spout
	public void declareOutputFields(OutputFieldsDeclarer arg0)
	{
		// spout emits a tuple with a field named "Commit"
		arg0.declare(new Fields("Commit"));
	}

	public Map getComponentConfiguration()
	{
		return null;
	}
}
