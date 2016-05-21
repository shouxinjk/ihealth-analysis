package com.shouxinjk.ihealth.analyzer.spout;

import java.util.Map;
import java.util.Random;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;;

public class GuideLineRuleSpout  extends BaseRichSpout implements IRichSpout{
    private SpoutOutputCollector collector;
    private static String[] words = {"happy","excited","angry"};
    
    
    /* (non-Javadoc)
     * @see backtype.storm.spout.ISpout#nextTuple()
     */
    public void nextTuple() {
        // TODO Auto-generated method stub
        String word = words[new Random().nextInt(words.length)]; 
        collector.emit(new Values(word));
    }
    

	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector arg2) {
		// TODO Auto-generated method stub
		this.collector = arg2;
	}

	public void declareOutputFields(OutputFieldsDeclarer arg0) {
		// TODO Auto-generated method stub
		arg0.declare(new Fields("randomstring"));
	}
}