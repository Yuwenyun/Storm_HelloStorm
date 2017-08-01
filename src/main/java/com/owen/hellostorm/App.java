package com.owen.hellostorm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * This is a demo project showing how to create a simple storm project.
 * The whole project consists of one spout and two bolts
 */
public class App 
{
	private static final int TEN_SECONDS = 100_000;
	
    public static void main( String[] args )
    {
    	// topology builder is used to wire together the spout and bolts
        TopologyBuilder builder = new TopologyBuilder();
        
        // set spout with id "commit-feed-listener"
        builder.setSpout("commit-feed-listener", new CommitFeedListener());
        builder.setBolt("email-extractor", new EmailExtractor()).shuffleGrouping("commit-feed-listener");
        builder.setBolt("email-counter", new EmailCounter()).fieldsGrouping("email-extractor", new Fields("Email"));
        
        Config config = new Config();
        config.setDebug(true);
        
        // build storm topology from builder
        StormTopology topology = builder.createTopology();
        
        // run the topology in local cluster
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("commit-count-topology", config, topology);
        
        // shutdown the cluster, or it will run forever
        Utils.sleep(TEN_SECONDS);
        cluster.killTopology("commit-count-topology");
        cluster.shutdown();
    }
}
