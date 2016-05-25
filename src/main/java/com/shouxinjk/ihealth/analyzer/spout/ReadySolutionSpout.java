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

public class ReadySolutionSpout extends BaseRichSpout implements IRichSpout {
    boolean isDistributed;
    SpoutOutputCollector collector;
    Integer queryTimeoutSecs;
    protected transient JdbcClient jdbcClient;
    protected ConnectionProvider connectionProvider;
    public List<Column> columns;
    private static final Logger logger = Logger.getLogger(ReadySolutionSpout.class);
    
    public ReadySolutionSpout(ConnectionProvider connectionProvider) {
        this(connectionProvider,true);
    }
    
    public ReadySolutionSpout(ConnectionProvider connectionProvider,boolean isDistributed) {
        this.isDistributed = isDistributed;
        this.connectionProvider = connectionProvider;
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
        columns.add(new Column("constant", 1, Types.INTEGER));
    }

    public void close(){
    	connectionProvider.cleanup();
    }

    public void nextTuple() {
        String sql="select checkupPackage_id as user_id,checkupPackage_id,if(matchedRules>generatedRules,'inprocess','ready') as status from ta_statistics where status='inprocess' and 1=?";
        List<List<Column>> result = jdbcClient.select(sql,columns);
        if (result != null && result.size() != 0) {
            for (List<Column> row : result) {
                Values values = new Values();
                String userId=row.get(0).getVal().toString();//get userId
                String checkupPakcageId=row.get(1).getVal().toString();//get checkup package id
                String status=row.get(2).getVal().toString();//get status
                for(Column column : row) {
                    values.add(column.getVal());
                }
                //here we update timestamp
                String updateTimestampSql = "update ta_user set lastEvaluatedOn=now() where user_id='"+userId+"'";
                logger.debug("Try to update user status.[SQL]"+updateTimestampSql);
                jdbcClient.executeSql(updateTimestampSql); 
                //here we update statistic matchedRules
                String statisticSql = "update ta_statistics set status='"+status+"' where checkuppackage_id='"+checkupPakcageId+"'";
                logger.debug("Try to update satistic status.[SQL]"+statisticSql);
                jdbcClient.executeSql(statisticSql); 
                this.collector.emit(values);
            }
        }
    }


    public void ack(Object msgId) {
    	//do nothing
    }

    public void fail(Object msgId) {
    	//do nothing
    }

    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("user_id","checkupPackage_id","status"));
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
