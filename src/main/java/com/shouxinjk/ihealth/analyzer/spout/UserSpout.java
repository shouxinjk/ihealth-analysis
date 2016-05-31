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

import org.apache.log4j.Logger;
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
    public List<Column> columns;
//    String timestampFieldA = "lastModifiedOn";
//    String timestampFieldB = "lastEvaluatedOn";
    private static final Logger logger = Logger.getLogger(UserSpout.class);
    
    public UserSpout(ConnectionProvider connectionProvider) {
        this(connectionProvider,"lastModifiedOn","lastEvaluatedOn");
    }
    
    public UserSpout(ConnectionProvider connectionProvider,String timestampField1,String timestampField2) {
        this.isDistributed = true;
        this.connectionProvider = connectionProvider;
//        this.timestampFieldA = timestampField1;
//        this.timestampFieldB = timestampField2;
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
        columns.add(new Column("timestamp1", 1, Types.INTEGER));
    }

    public void close(){
    	//TODO need to check if other bolts will be impacted
    	connectionProvider.cleanup();
    }

    public void nextTuple() {
    	//select user_id,user_id as checkuppackage_id from ta_user where lastModifiedOn>lastEvaluatedOn
        String sql = "select user_id,user_id as checkuppackage_id from ta_user where lastModifiedOn>lastEvaluatedOn and ?";
        System.err.println("try to query candidate users.[SQL]"+sql);
        List<List<Column>> result = jdbcClient.select(sql,columns);
        if (result != null && result.size() != 0) {
            for (List<Column> row : result) {
                Values values = new Values();
                String userId=row.get(0).getVal().toString();//get UserId
                for(Column column : row) {
                    values.add(column.getVal());
                }
                //update checkuppackage status
        		StringBuffer sb = new StringBuffer();
        		sb = new StringBuffer("insert into tb_checkuppackage (checkuppackage_id,status,user_id,createby,createon) values('");
        		sb.append(userId);
        		sb.append("','pending','");
        		sb.append(userId);
        		sb.append("','robot',now()) ");
        		sb.append("on duplicate key update status='pending'");
        		sql = sb.toString();
        		logger.debug("Try to insert/update checkup package.[SQL]"+sql);
        		jdbcClient.executeSql(sql);
        		//delete obsoleted checkup items
        		sql = "delete from tb_checkupitem where user_id='"+userId+"'";
        		logger.debug("Try to delete obsolete checkup items.[SQL]"+sql);
        		jdbcClient.executeSql(sql);
                //here we update timestamp
                String updateTimestampSql = "update ta_user set lastEvaluatedOn=now() where user_id='"+userId+"'";
                System.err.println("try to update user timestamp.[SQL]"+updateTimestampSql);
                jdbcClient.executeSql(updateTimestampSql); 
                this.collector.emit(values);
            }
        }
        Thread.yield();
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
