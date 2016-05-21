/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.shouxinjk.ihealth.analyzer.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import com.google.common.collect.Lists;

import java.util.*;

public class CheckupPackageSpout extends BaseRichSpout implements IRichSpout {
    boolean isDistributed;
    SpoutOutputCollector collector;
    public static final List<Values> rows = Lists.newArrayList(
            new Values("0453deafa8114ce9a55f9245a87b709c","0453deafa8114ce9a55f9245a87b709c"),
            new Values("0e589df1ee70439e9af5a11e642dd1c7","0e589df1ee70439e9af5a11e642dd1c7"));
    public List<Values> evaluatedValues = Lists.newArrayList();
    
    
    public CheckupPackageSpout() {
        this(true);
    }

    public CheckupPackageSpout(boolean isDistributed) {
        this.isDistributed = isDistributed;
    }

    public boolean isDistributed() {
        return this.isDistributed;
    }

    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
    }

    public void close() {

    }

    public void nextTuple() {
//        final Random rand = new Random();
//        final Values row = rows.get(rand.nextInt(rows.size() - 1));
//        this.collector.emit(row);
//      Thread.yield();
    	
    	//TODO here we should query ta_user by lastModfiedOn>lastEvaluatedOn
        for(Values value:rows){
        	if(this.evaluatedValues.contains(value)){
        		//we have emitted this value. and then we do nothing
        	}else{
	        	this.collector.emit(value);
	        	this.evaluatedValues.add(value);
        	}
        }
    }

    public void ack(Object msgId) {
    	//TODO here we should update ta_user.lastEvaluatedOn
    }

    public void fail(Object msgId) {
    	//do nothing
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("user_id","checkuppackage_id"));
    }

    @Override
    public void activate() {
    }

    @Override
    public void deactivate() {
    }

    @Override
    public Map<String, Object> getComponentConfiguration() {
        return null;
    }
}
