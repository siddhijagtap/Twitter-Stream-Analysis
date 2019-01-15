/**
 * 
 */
package TWITTER;

import java.io.File;
import java.io.PrintWriter;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;

import twitter4j.HashtagEntity;
import twitter4j.StallWarning;
import twitter4j.Status;
import twitter4j.StatusDeletionNotice;
import twitter4j.StatusListener;
import twitter4j.TwitterStream;
import twitter4j.TwitterStreamFactory;
import twitter4j.auth.AccessToken;
import twitter4j.conf.ConfigurationBuilder;
import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import backtype.storm.utils.Utils;



public class ReadStreamSpout extends BaseRichSpout{
	
	private SpoutOutputCollector _collector; 
	private LinkedBlockingQueue<Status> _msgs;
//	private Map<Long,Set<String>> _timeStampTweetTagMap = null;
	private TwitterStream _stream;
//	private PrintWriter _fw;
	
    
	@Override
	public void nextTuple() {
		// TODO Auto-generated method stub
		Status s = _msgs.poll();
		
        if (s == null) {
//            Utils.sleep(1000);
        	try{
        		Thread.sleep(100);
        	} catch(Exception e){
        		e.printStackTrace();
        	}
        } else {
            _collector.emit(new Values(s));
//            System.out.println("************** IN SPOUT ***************** :: "+s);

        }
		
	}

	@Override
	public void open(Map config, TopologyContext context, SpoutOutputCollector collector) {
		// TODO Auto-generated method stub
		
		_msgs = new LinkedBlockingQueue<Status>();
		this._collector = collector; 
		
//		try{
//			_fw = new PrintWriter(new File("/s/chopin/k/grad/amchakra/Tweetlog.txt"));
//		}catch (Exception e) {
//			System.out.println("UNABLE TO WRITE FILE :: 1 ");
//			e.printStackTrace();
//		}
		
		
		StatusListener listener = new StatusListener(){
			@Override
			public void onStatus(Status status) {
//	            System.out.println("!!!!!!!!!! ---> "+status.getUser().getName() + " : " + status.getText());
	        	_msgs.offer(status); 
//	        	String toWrite = String.valueOf(status.getCreatedAt().getTime())+" :::: ";
	        	/*for (HashtagEntity hstg : status.getHashtagEntities()) {
//	        		toWrite += "<"+hstg.getText()+">";
	        		long crtdTime = status.getCreatedAt().getTime();
	        		if (_timeStampTweetTagMap.containsKey(crtdTime)) {
	        			
	        			Set<String> prevtags = _timeStampTweetTagMap.get(crtdTime);
	        			prevtags.add("<"+hstg.getText()+">");
	        			_timeStampTweetTagMap.put(crtdTime, prevtags);
	        			
	        		} else {
	        			Set<String> tags = new HashSet<String>();
	        			tags.add("<"+hstg.getText()+">");
	        			_timeStampTweetTagMap.put(crtdTime, tags);
	        		}
	        	}
	        	
	        	for(long time : _timeStampTweetTagMap.keySet()) {
	        		String toWrite = String.valueOf(time)+" :::: ";
	        		for(String tg : _timeStampTweetTagMap.get(time)) {
	        			toWrite += tg;
	        		}
	        		_fw.write(toWrite+"\n");
	        		System.out.println("TWEET :::: "+toWrite);
	        		_timeStampTweetTagMap.remove(time);
	        	}*/
	        	
	        }
	        
			@Override
			public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
//				System.out.println("************* IN DELETION NOTICE ***********************");
			}
	        
			@Override
	        public void onTrackLimitationNotice(int numberOfLimitedStatuses) {
//				System.out.println("************* IN TRACk LIMITATION NOTICE ***********************");
			}
	        
			@Override
	        public void onException(Exception ex) {
	            ex.printStackTrace();
	        }
			
			@Override
			public void onScrubGeo(long arg0, long arg1) {
				// TODO Auto-generated method stub
				
			}
			@Override
			public void onStallWarning(StallWarning arg0) {
				// TODO Auto-generated method stub
				
			}
		};
		
		ConfigurationBuilder cb = new ConfigurationBuilder();
        cb.setDebugEnabled(true);
        cb.setJSONStoreEnabled(true);
        
        cb.setOAuthConsumerKey("rm0NYAl2RoDFEV4AkqiR9AjNZ");
        cb.setOAuthConsumerSecret("TEOqkyZDBMRW3cQLWEmwX9BXBeUnq35rLxpaNrvaQhoGv6Kwbi");
        cb.setOAuthAccessToken("4130873239-aDi90Ihhd4kVUNgNfg7kxRuZNPal8URcyXfCHEw");
        cb.setOAuthAccessTokenSecret("VB1qdjU4W6E6fDfP0swLaMBIhNcDoB34sOq3SQTKe5VvS");
        
		TwitterStream twitterStream = new TwitterStreamFactory(cb.build()).getInstance();
		
		twitterStream.addListener(listener);
		twitterStream.sample();
        System.out.println("Connect Successful");
        
//		twitterStream.setOAuthConsumer("rm0NYAl2RoDFEV4AkqiR9AjNZ", "TEOqkyZDBMRW3cQLWEmwX9BXBeUnq35rLxpaNrvaQhoGv6Kwbi");
//		AccessToken token = new AccessToken("4130873239-aDi90Ihhd4kVUNgNfg7kxRuZNPal8URcyXfCHEw", "VB1qdjU4W6E6fDfP0swLaMBIhNcDoB34sOq3SQTKe5VvS");
//		twitterStream.setOAuthAccessToken(token);
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		// TODO Auto-generated method stub
		declarer.declare(new Fields("tweet-sentence"));
		
	}
	
	/*public void cleanup() {  
		
		_fw.close();
	}*/

}
