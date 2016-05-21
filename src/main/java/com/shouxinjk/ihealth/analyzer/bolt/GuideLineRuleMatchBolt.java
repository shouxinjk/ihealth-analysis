package com.shouxinjk.ihealth.analyzer.bolt;

import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

public class GuideLineRuleMatchBolt  extends BaseBasicBolt{

	public void execute(Tuple tuple, BasicOutputCollector collector) {
		// TODO Auto-generated method stub
        String word = (String) tuple.getValue(0);  
        String out = "I'm " + word +  "!";  
        System.out.println("out=" + out);		
	}

	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		// TODO Auto-generated method stub
		
	}
}
