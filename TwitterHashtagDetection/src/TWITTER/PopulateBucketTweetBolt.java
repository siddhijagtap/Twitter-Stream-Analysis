/**
 * 
 */
package TWITTER;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import twitter4j.HashtagEntity;
import twitter4j.Status;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;



public class PopulateBucketTweetBolt extends BaseRichBolt{

	private int _bucketID = 0;
	private int _bucktthresholdcnt = 0;
	private List<Object> _tweets = null; 
	private OutputCollector _collector = null;
	private Map<String,BucketpacketInfo> _Dbucket = null; 
	private List<BucketpacketInfo> _perBucketproperTags = null;
	private int _bucketSize = 0;
	
	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		Object oo = tuple.getValueByField("tweet-sentence");
		_tweets.add(oo);

		_collector.ack(tuple);
		
		Status stats = (Status) oo;
		
		for (HashtagEntity hstg : stats.getHashtagEntities()) {
			
			String hashTag = hstg.getText().toLowerCase();
			

			
			if(!hashTag.isEmpty()) {
				if(_Dbucket.containsKey(hashTag)) { // Increase Frequency
					
					BucketpacketInfo prevInfo = _Dbucket.get(hashTag);
					prevInfo.setFrequency(prevInfo.getFrequency()+1); // Updated Frequency

					_Dbucket.put(hashTag, prevInfo); // overwriting again
					
					
				} else { // Just Push Info with proper values
					BucketpacketInfo info = new BucketpacketInfo(hashTag,1,(_bucketID-1));
					_Dbucket.put(hashTag, info);
				}
				
				_bucktthresholdcnt++;
			}

			
			if(_bucktthresholdcnt%_bucketSize == 0) { // Reached Threshold : Bucket Size : 5
				
				for(String hshtg : _Dbucket.keySet()) {
					BucketpacketInfo bcktInfo = _Dbucket.get(hshtg);
					

					
					if(bcktInfo.getFrequency()+bcktInfo.getMaxError() > _bucketID) { // Keep Them and Emit them to DumpLogger
//						_perBucketproperTags.add(bcktInfo);
//						System.out.println("*************** HASTAG : "+bcktInfo.getHashTag()+" : FREQUENCY : "+String.valueOf(bcktInfo.getFrequency())+" : ACTUAL FREQUENCY : "+String.valueOf(bcktInfo.getFrequency()+bcktInfo.getMaxError()));
						_collector.emit(new Values(bcktInfo.getHashTag(), bcktInfo.getFrequency(), (bcktInfo.getFrequency()+bcktInfo.getMaxError())));
					} else { // Delete Them from Bucket  
						_Dbucket.remove(hshtg);
					}
				}
				
/
				_bucketID++;
			}
		}
		
	
	}

	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		
		_bucketID = 1;
		_bucketSize = 50;
		_bucktthresholdcnt = 0;
		_collector = collector;
		_tweets = new ArrayList<Object>();
		_Dbucket = new ConcurrentHashMap<String, BucketpacketInfo>();	
		_perBucketproperTags = new ArrayList<BucketpacketInfo>();
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		
		declarer.declare(new Fields("tag","count","actual-count"));
		
	}
	

}
