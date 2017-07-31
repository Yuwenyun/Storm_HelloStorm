package com.owen.hellostorm;

import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * Hello world!
 *
 */
public class App 
{
	private static final int TEN_SECONDS = 100_000;
	
    public static void main( String[] args )
    {
        TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout("commit-feed-listener", new CommitFeedListener());
        builder.setBolt("email-extractor", new EmailExtractor()).shuffleGrouping("commit-feed-listener");
        builder.setBolt("email-counter", new EmailCounter()).fieldsGrouping("email-extractor", new Fields("Email"));
        
        Config config = new Config();
        config.setDebug(true);
        
        StormTopology topology = builder.createTopology();
        LocalCluster cluster = new LocalCluster();
        cluster.submitTopology("commit-count-topology", config, topology);
        
        Utils.sleep(TEN_SECONDS);
        cluster.killTopology("commit-count-topology");
        cluster.shutdown();
    }
}
