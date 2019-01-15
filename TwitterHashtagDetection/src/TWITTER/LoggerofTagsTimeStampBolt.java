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

import twitter4j.HashtagEntity;
import twitter4j.Status;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;


public class LoggerofTagsTimeStampBolt extends BaseRichBolt{

	private Map<Long,Set<String>> _timeStampTweetTagMap = null;
	private PrintWriter _fw;
	
	@Override
	public void execute(Tuple tuple) {
		// TODO Auto-generated method stub
		Object oo = tuple.getValueByField("tweet-sentence");
		Status status = (Status) oo;
		for (HashtagEntity hstg : status.getHashtagEntities()) {
//    		toWrite += "<"+hstg.getText()+">";
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
    		_fw.flush();
    		System.out.println("TWEET :::: "+toWrite);
    		_timeStampTweetTagMap.remove(time);
    	}
	}

	@Override
	public void prepare(Map config, TopologyContext context, OutputCollector collector) {
		// TODO Auto-generated method stub
		_timeStampTweetTagMap = new HashMap<Long, Set<String>>();
		try{
			_fw = new PrintWriter(new File("/s/chopin/k/grad/amchakra/Tweetlog.txt"));
		}catch (Exception e) {
			System.out.println("UNABLE TO WRITE FILE :: 1 ");
			e.printStackTrace();
		}
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		
	}
	
	public void cleanup() {  
		
		_fw.close();
	}
}
