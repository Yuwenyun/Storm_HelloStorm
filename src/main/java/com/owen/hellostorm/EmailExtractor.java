package com.owen.hellostorm;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;

public class EmailExtractor implements IRichBolt
{
	static final long serialVersionUID = 1L;
	private OutputCollector collector;

	public void cleanup(){}
	
	// get called when Storm prepares to start the bolt
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2)
	{
		this.collector = arg2;
	}
	
	// get called when a tuple has been emitted to this bolt
	public void execute(Tuple arg0)
	{
		// get the field with it's name
		String commit = arg0.getStringByField("Commit");
		String[] parts = commit.split(" ");
		this.collector.emit(new Values(parts[1]));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0)
	{
		// this bolt will emit a tuple with a field named "Email"
		arg0.declare(new Fields("Email"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration(){ return null; }

}
