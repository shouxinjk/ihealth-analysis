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
package com.shouxinjk.ihealth.analyzer.topology;

import org.apache.storm.cassandra.bolt.BaseCassandraBolt;
import org.apache.storm.cassandra.bolt.CassandraWriterBolt;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;

import com.google.common.collect.Lists;
import com.shouxinjk.ihealth.analyzer.spout.UserRuleSpout;
import com.shouxinjk.ihealth.analyzer.util.Util;

import org.apache.storm.jdbc.bolt.JdbcInsertBolt;
import org.apache.storm.jdbc.bolt.JdbcLookupBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.mapper.JdbcLookupMapper;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcLookupMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;

import static org.apache.storm.cassandra.DynamicStatementBuilder.all;
import static org.apache.storm.cassandra.DynamicStatementBuilder.async;
import static org.apache.storm.cassandra.DynamicStatementBuilder.simpleQuery;

import java.sql.Types;
import java.util.List;


public class MatchTopology extends AbstractCheckupSolutionTopology {
    private static final String USER_RULE_SPOUT = "USER_RULE_SPOUT";
    private static final String SQL_FIND_USER_RULE_BOLT = "SQL_FIND_USER_RULE_BOLT";
    private static final String SQL_MATCH_USER_RULE_BOLT = "SQL_MATCH_USER_RULE_BOLT";
    private static final String SQL_UPDATE_USER_RULE_BOLT = "SQL_UPDATE_USER_RULE_BOLT";
    private static final String SQL_UPDATE_LAST_MATCHED_TIME = "SQL_UPDATE_LAST_MATCHED_TIME";
    
    private static final String SQL_MATCH_USER_RULE="select ? as user_id,? as rule_id,if(count(*)>0,'match','dismatch') as status from ta_user where user_id=? and ?";
    public static void main(String[] args) throws Exception {
        new MatchTopology().execute(args);
    }

    @Override
    public StormTopology getTopology() {
    	  	
    	UserRuleSpout userRuleSpout = new UserRuleSpout(connectionProvider);
    	    	
        //SQL:select all UserRule
    	//select user_id,rule_id,guideline_id,ruleExpression from ta_userRule where user_id=$user_id;
//    	String sqlFindUserRule = prop.getProperty("mysql.query.userRule", SQL_FIND_USER_RULE);
//        Fields userRuleOutputFields = new Fields("user_id","rule_id","user_id2","ruleExpression");//Here we query all pending user rules
//        List<Column> queryUserRuleParams = Lists.newArrayList(new Column("user_id", Types.VARCHAR));
//        JdbcLookupMapper jdbcUserRuleLookupMapper = new SimpleJdbcLookupMapper(userRuleOutputFields, queryUserRuleParams);
//        JdbcLookupBolt jdbcFindUserRuleBolt = new JdbcLookupBolt(connectionProvider, sqlFindUserRule, jdbcUserRuleLookupMapper);

        //SQL:check if user-rule match
        //select “$user_id"as user_id,“$rule_id" as rule_id,if(count(*)>0,“match",“dismatch") as status from ta_user where user_id=$user_id and $ruleExpression
    	String sqlMatchUserRule = prop.getProperty("mysql.query.userRule.match", SQL_MATCH_USER_RULE);
        Fields userRuleMatchResultFields = new Fields("user_id","rule_id","status");//Here we get match results
        List<Column> matchUserRuleParams = Lists.newArrayList(new Column("user_id", Types.VARCHAR),
        		new Column("rule_id", Types.VARCHAR),
        		new Column("user_id2", Types.VARCHAR),
        		new Column("ruleExpression", Types.VARCHAR));
        JdbcLookupMapper jdbcUserRuleMatchMapper = new SimpleJdbcLookupMapper(userRuleMatchResultFields, matchUserRuleParams);
        JdbcLookupBolt jdbcMatchUserRuleBolt = new JdbcLookupBolt(connectionProvider, sqlMatchUserRule, jdbcUserRuleMatchMapper);
        
        //SQL:update match results
        //update ta_userRule set status=“$status” where user_id=“$user_id” and rule_id=“$rule_id”
        List<Column> schemaColumns = Lists.newArrayList(new Column("status", Types.VARCHAR),
        		new Column("user_id", Types.VARCHAR),
        		new Column("rule_id", Types.VARCHAR));//used for query values from tuple
        JdbcMapper mapper = new SimpleJdbcMapper(schemaColumns);//define tuple columns
        JdbcInsertBolt jdbcUpdateMatchResultBolt = new JdbcInsertBolt(connectionProvider, mapper)
                .withInsertQuery("update ta_userRule set sysflag='toGenerate',status=? where user_id=? and rule_id=?");
        
        //SQL: update lastPreparedOn timestamp
        List<Column> timestampSchemaColumns = Lists.newArrayList(new Column("user_id", Types.VARCHAR));
        JdbcMapper timestampMapper = new SimpleJdbcMapper(timestampSchemaColumns);//define tuple columns
        JdbcInsertBolt jdbcUpdateUserTimestampBolt = new JdbcInsertBolt(connectionProvider, timestampMapper)
                .withInsertQuery("update ta_user set lastMatchedOn=now(),status='matched' where user_id=?");
        
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(USER_RULE_SPOUT, userRuleSpout, 1);//TODO here we should put candidate user in a queue like Kafka
//        builder.setBolt(SQL_FIND_USER_RULE_BOLT, jdbcFindUserRuleBolt, 1).shuffleGrouping(USER_RULE_SPOUT);
        builder.setBolt(SQL_MATCH_USER_RULE_BOLT, jdbcMatchUserRuleBolt, 1).shuffleGrouping(USER_RULE_SPOUT);
        builder.setBolt(SQL_UPDATE_LAST_MATCHED_TIME, jdbcUpdateUserTimestampBolt, 1).shuffleGrouping(SQL_MATCH_USER_RULE_BOLT);
        builder.setBolt(SQL_UPDATE_USER_RULE_BOLT, jdbcUpdateMatchResultBolt, 1).shuffleGrouping(SQL_MATCH_USER_RULE_BOLT);
        return builder.createTopology();
    }
}
