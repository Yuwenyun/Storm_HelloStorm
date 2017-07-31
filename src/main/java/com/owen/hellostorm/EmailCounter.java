package com.owen.hellostorm;

import java.util.HashMap;
import java.util.Map;

import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichBolt;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Tuple;

public class EmailCounter implements IRichBolt
{
	private static final long serialVersionUID = 1L;
	private Map<String, Integer> counts;

	@Override
	public void cleanup(){}
	// no following bolt, so no tuple emitted from this bolt, no need to declare
	// tuple field name
	public void declareOutputFields(OutputFieldsDeclarer arg0){}

	@Override
	public void execute(Tuple arg0)
	{
		String email = arg0.getStringByField("Email");
		counts.put(email, countFor(email));
		printCounts();
	}

	private Integer countFor(String email)
	{
		Integer count = counts.get(email);
		return count == null ? 1 : count + 1;
	}

	private void printCounts()
	{
		for(String email : counts.keySet())
		{
			System.out.println(String.format("%s : %s", email, counts.get(email)));
		}
	}

	@Override
	public void prepare(Map arg0, TopologyContext arg1, OutputCollector arg2)
	{
		counts = new HashMap<String, Integer>();
	}

	@Override
	public Map<String, Object> getComponentConfiguration()
	{
		return null;
	}
}
