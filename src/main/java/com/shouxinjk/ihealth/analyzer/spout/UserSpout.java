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

import org.apache.storm.Config;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.JdbcClient;
import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.IRichSpout;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import com.google.common.collect.Lists;

import java.sql.Types;
import java.util.*;

public class UserSpout extends BaseRichSpout implements IRichSpout {
    boolean isDistributed;
    SpoutOutputCollector collector;
    Integer queryTimeoutSecs;
    protected transient JdbcClient jdbcClient;
    protected ConnectionProvider connectionProvider;
    String sql = "select user_id,user_id as checkuppackage_id from ta_user where ?>?";
    public List<Column> columns;
    String timestampFieldA = "lastModifiedOn";
    String timestampFieldB = "lastEvaluatedOn";
    
    public static final List<Values> rows = Lists.newArrayList(
            new Values("0453deafa8114ce9a55f9245a87b709c","0453deafa8114ce9a55f9245a87b709c"),
            new Values("0e589df1ee70439e9af5a11e642dd1c7","0e589df1ee70439e9af5a11e642dd1c7"));
    public List<Values> evaluatedValues = Lists.newArrayList();
    
    
    //TODO this should be disabled
    public UserSpout() {
//        this(true);
    }
    
    public UserSpout(ConnectionProvider connectionProvider,String timestampField1,String timestampField2) {
        this.isDistributed = true;
        this.connectionProvider = connectionProvider;
        this.timestampFieldA = timestampField1;
        this.timestampFieldB = timestampField2;
        this.columns = new ArrayList<Column>();
    }

    public boolean isDistributed() {
        return this.isDistributed;
    }

    @SuppressWarnings("rawtypes")
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        this.collector = collector;
        if(queryTimeoutSecs == null) {
            queryTimeoutSecs = Integer.parseInt(conf.get(Config.TOPOLOGY_MESSAGE_TIMEOUT_SECS).toString());
        }
        connectionProvider.prepare();
        this.jdbcClient = new JdbcClient(connectionProvider, queryTimeoutSecs);
        columns.add(new Column("timestamp1", timestampFieldA, Types.VARCHAR));
        columns.add(new Column("timestamp2", timestampFieldB, Types.VARCHAR));
    }

    public void close(){
    	//TODO need to check if other bolts will be impacted
    	connectionProvider.cleanup();
    }

    public void nextTuple() {
//        final Random rand = new Random();
//        final Values row = rows.get(rand.nextInt(rows.size() - 1));
//        this.collector.emit(row);
//      Thread.yield();
    	
    	//TODO here we should query ta_user by lastModfiedOn>lastEvaluatedOn
    	//TEST: we use static data
//        for(Values value:rows){
//        	if(this.evaluatedValues.contains(value)){
//        		//we have emitted this value. and then we do nothing
//        	}else{
//	        	this.collector.emit(value);
//	        	this.evaluatedValues.add(value);
//        	}
//        }

//        List<Column> columns = Lists.newArrayList(new Column("timestamp1", Types.VARCHAR),new Column("timestamp2", Types.VARCHAR));
        List<List<Column>> result = jdbcClient.select(sql,columns);
        if (result != null && result.size() != 0) {
            for (List<Column> row : result) {
                Values values = new Values();
                String userId="-1";
                for(Column column : row) {
                    values.add(column.getVal());
                    userId=column.getVal().toString();
                }
                String updateTimestampSql = "update ta_user set "+timestampFieldB+"=now() where user_id='"+userId+"'";
//                System.err.println(updateTimestampSql);
                jdbcClient.executeSql(updateTimestampSql); 
                this.collector.emit(values);
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
//        declarer.declare(new Fields("user_id"));
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
