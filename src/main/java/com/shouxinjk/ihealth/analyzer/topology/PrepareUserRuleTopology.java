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
import com.shouxinjk.ihealth.analyzer.spout.UserSpout;
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

/**
 * prepare data for analyzing
 * 1, generate checkupPackage status OR update checkupPackage status
 * 2, update pending user rules
 * 
 * @author qchzhu
 * 
 */
public class PrepareUserRuleTopology extends AbstractCheckupSolutionTopology {
    private static final String USER_SPOUT = "USER_SPOUT";
    private static final String SQL_FIND_ALL_GUIDELINE_BOLT = "SQL_FIND_ALL_GUIDELINE_BOLT";
    private static final String SQL_INSERT_HIGHRISK_USER_RULE_BOLT = "SQL_INSERT_HIGHRISK_USER_RULE_BOLT";
    private static final String SQL_INSERT_LOWRISK_USER_RULE_BOLT = "SQL_INSERT_LOWRISK_USER_RULE_BOLT";
    private static final String SQL_UPDATE_LAST_PREPARED_TIME = "SQL_UPDATE_LAST_PREPARED_TIME";
    
    //TODO here must append guideline status
    private static final String SQL_FIND_GUIDELINE_DEBUG="SELECT concat('high-',a.examguideline_id) as highrisk_ruleid,concat('low-',a.examguideline_id) as lowrisk_ruleid,? as user_id,a.examguideline_id,a.originate,a.concernedfactors,a.description,a.highriskdefine,a.highriskexpression,a.lowriskdefine,a.lowriskexpression,b.NAME as disease_name from exam_examguideline a left join admin_disease b on a.DISEASE_ID=b.DISEASE_ID";
    private static final String SQL_FIND_GUIDELINE="SELECT concat('high-',a.examguideline_id) as highrisk_ruleid,concat('low-',a.examguideline_id) as lowrisk_ruleid,? as user_id,a.examguideline_id,a.originate,a.concernedfactors,a.description,a.highriskdefine,a.highriskexpression,a.lowriskdefine,a.lowriskexpression,b.NAME as disease_name from exam_examguideline a left join admin_disease b on a.DISEASE_ID=b.DISEASE_ID where a.status='5'";
        
    public static void main(String[] args) throws Exception {
        new PrepareUserRuleTopology().execute(args);
    }

    @Override
    public StormTopology getTopology() {
    	
    	//Stream: get stream data from Queue like Kafka
    	UserSpout userSpout = new UserSpout(connectionProvider,"lastModifiedOn","lastEvaluatedOn");
    	
        //SQL:select all users that don't have checkup package
    	String sql = SQL_FIND_GUIDELINE;
    	if(!"production".equalsIgnoreCase(prop.getProperty("common.mode", "production")))//ignore status under debug mode
            sql = SQL_FIND_GUIDELINE_DEBUG;
        Fields outputFields = new Fields("highrisk_ruleid","lowrisk_ruleid","user_id","examguideline_id","originate","concernedfactors","description",
        		"highriskdefine","highriskexpression","lowriskdefine","lowriskexpression","disease_name");//Here we query all guidelines
        List<Column> queryParamColumns = Lists.newArrayList(new Column("user_id", Types.VARCHAR));
        JdbcLookupMapper jdbcLookupMapper = new SimpleJdbcLookupMapper(outputFields, queryParamColumns);
        JdbcLookupBolt jdbcFindNewUserBolt = new JdbcLookupBolt(connectionProvider, sql, jdbcLookupMapper);

        //SQL:insert high risk rules
        List<Column> highRiskSchemaColumns = Lists.newArrayList(new Column("highrisk_ruleid", Types.VARCHAR),
        		new Column("user_id", Types.VARCHAR),
        		new Column("examguideline_id", Types.VARCHAR),
        		new Column("disease_name", Types.VARCHAR),
        		new Column("originate", Types.VARCHAR),
        		new Column("description", Types.VARCHAR),
        		new Column("concernedfactors", Types.VARCHAR),
        		new Column("highriskdefine", Types.VARCHAR),
        		new Column("highriskexpression", Types.VARCHAR));//used for query values from tuple
        JdbcMapper highRiskUserRuleMapper = new SimpleJdbcMapper(highRiskSchemaColumns);//define tuple columns
        JdbcInsertBolt jdbcInsertHighRiskUserRuleBolt = new JdbcInsertBolt(connectionProvider, highRiskUserRuleMapper)
                .withInsertQuery("insert into ta_userRule(rule_id,user_id,guideline_id,disease_name,originate,description,concernedfactors,riskDefine,ruleExpression,riskType,status,createdOn,modifiedOn) "
                		+ "values (?,?,?,?,?,?,?,?,?,'high','pending',now(),now()) on duplicate key update status='pending',modifiedOn=now()");
                
        //SQL:insert pending guide rules
        List<Column> lowRiskSchemaColumns = Lists.newArrayList(new Column("lowrisk_ruleid", Types.VARCHAR),
        		new Column("user_id", Types.VARCHAR),
        		new Column("examguideline_id", Types.VARCHAR),
        		new Column("disease_name", Types.VARCHAR),
        		new Column("originate", Types.VARCHAR),
        		new Column("description", Types.VARCHAR),
        		new Column("concernedfactors", Types.VARCHAR),
        		new Column("highriskdefine", Types.VARCHAR),
        		new Column("highriskexpression", Types.VARCHAR));//used for query values from tuple
        JdbcMapper lowRiskUserRuleMapper = new SimpleJdbcMapper(lowRiskSchemaColumns);//define tuple columns
        JdbcInsertBolt jdbcInsertLowRiskUserRuleBolt = new JdbcInsertBolt(connectionProvider, lowRiskUserRuleMapper)
                .withInsertQuery("insert into ta_userRule(rule_id,user_id,guideline_id,disease_name,originate,description,concernedfactors,riskDefine,ruleExpression,riskType,status,createdOn,modifiedOn) "
                		+ "values (?,?,?,?,?,?,?,?,?,'low','pending',now(),now()) on duplicate key update status='pending'");
                
        //SQL: update lastPreparedOn timestamp
        List<Column> timestampSchemaColumns = Lists.newArrayList(new Column("user_id", Types.VARCHAR));
        JdbcMapper timestampMapper = new SimpleJdbcMapper(timestampSchemaColumns);//define tuple columns
        JdbcInsertBolt jdbcUpdateUserTimestampBolt = new JdbcInsertBolt(connectionProvider, timestampMapper)
                .withInsertQuery("update ta_user set lastPreparedOn=now() where user_id=?");
                
        //TOPO:userSpout ==> findNewUserBolt ==> insertCheckupPackageBolt
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(USER_SPOUT, userSpout, 1);//TODO here we should put candidate user in a queue like Kafka
        builder.setBolt(SQL_FIND_ALL_GUIDELINE_BOLT, jdbcFindNewUserBolt, 1).shuffleGrouping(USER_SPOUT);
        builder.setBolt(SQL_UPDATE_LAST_PREPARED_TIME, jdbcUpdateUserTimestampBolt, 1).shuffleGrouping(SQL_FIND_ALL_GUIDELINE_BOLT);
        builder.setBolt(SQL_INSERT_HIGHRISK_USER_RULE_BOLT, jdbcInsertHighRiskUserRuleBolt, 1).shuffleGrouping(SQL_FIND_ALL_GUIDELINE_BOLT);
        builder.setBolt(SQL_INSERT_LOWRISK_USER_RULE_BOLT, jdbcInsertLowRiskUserRuleBolt, 1).shuffleGrouping(SQL_FIND_ALL_GUIDELINE_BOLT);
        return builder.createTopology();
    }
}
