package com.shouxinjk.ihealth.analyzer.spout;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class NewUserQuerySpout extends BaseRichSpout {
	SpoutOutputCollector collector;
	
	@Override
	public void nextTuple() {
		Values values = new Values("select user_id from sys_app_user");//TODO to extract query SQL to property file
		this.collector.emit(values);
	}

	@Override
	public void open(Map map, TopologyContext context, SpoutOutputCollector collector) {
		this.collector = collector;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("user_id", "fname", "lname"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		// TODO Auto-generated method stub
		return null;
	}

}
