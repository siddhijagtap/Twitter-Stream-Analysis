package TWITTER;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;


public class Twittertopology {
	// ~/storm/apache-storm-0.9.5/bin/storm jar /s/chopin/k/grad/amchakra/cs_555_workspace/Sample_Storm_Test/target/Sample_Storm_Test-0.9.5-jar-with-dependencies.jar CS535.WordCountTopology
	
	// REMOTE
	// ~/storm/apache-storm-0.9.5/bin/storm jar ~/cs_555_workspace/Twitter_Data/target/Twitter_Data-0.9.5-jar-with-dependencies.jar TWITTER.Twittertopology remote
	// ~/storm/apache-storm-0.9.5/bin/storm jar ~/cs_555_workspace/Twitter_Data/target/Twitter_Data-0.9.5-jar-with-dependencies.jar TWITTER.Twittertopology remote-paral
	private static final String READSTREAM_SPOUT_ID = "stream-spout";
	private static final String POPULATEBUCKET_BOLT_ID = "populate-bolt";
	private static final String TWEETLOGOUTPUT_BOLT_ID = "tweet-log-bolt";
	private static final String LOGOUTPUT_BOLT_ID = "log-bolt";
	private static final String TOPOLOGY_NAME = "read-stream-topology";
	
	public static void main( String[] args) throws Exception { 
		
		ReadStreamSpout spout = new ReadStreamSpout();
		
		PopulateBucketTweetBolt populatebolt = new PopulateBucketTweetBolt();
		DumpLogofTagsBolt logbolt = new DumpLogofTagsBolt();
		LoggerofTagsTimeStampBolt twtLogBolt = new LoggerofTagsTimeStampBolt();
		
		TopologyBuilder builder = new TopologyBuilder(); 
		
		builder.setSpout(READSTREAM_SPOUT_ID, spout);
		builder.setBolt(POPULATEBUCKET_BOLT_ID, populatebolt).shuffleGrouping(READSTREAM_SPOUT_ID); 
		builder.setBolt(TWEETLOGOUTPUT_BOLT_ID, twtLogBolt).globalGrouping(READSTREAM_SPOUT_ID); 
		builder.setBolt(LOGOUTPUT_BOLT_ID, logbolt).fieldsGrouping(POPULATEBUCKET_BOLT_ID, new Fields("tag")); 
		
		Config config = new Config();
		config.setDebug(false); // true
		config.put(config.TOPOLOGY_MAX_SPOUT_PENDING, 1);
		
		if (args == null || args.length == 0) {
			
			LocalCluster cluster = new LocalCluster(); 
			cluster.submitTopology(TOPOLOGY_NAME, config, builder.createTopology()); 
			
			try {
				System.out.println("######################### ENTERING INTO SLEEP ##################################");
				
				Thread.sleep(1000000);
				
			} catch (Exception e) {
				System.out.println("######################### GETTING OUT OF SLEEP ##################################");
				e.printStackTrace();
			}
			
			cluster.killTopology(TOPOLOGY_NAME); 
			cluster.shutdown();
			
		} else if (args[0].equals("remote")){
			
			config.setNumWorkers(3);
		    StormSubmitter.submitTopologyWithProgressBar(args[0], config, builder.createTopology());
		    
		} else {
			System.out.println("PARALLEL");
		}
		
	}
}



