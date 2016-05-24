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
public class GenerateCheckupSolutionTopology extends AbstractCheckupSolutionTopology {
    private static final String USER_SPOUT = "USER_SPOUT";
    private static final String SQL_FIND_MATCHED_USERRULE_BOLT = "SQL_FIND_MATCHED_USERRULE_BOLT";
    private static final String SQL_FIND_MATCHED_SOLUTION_BOLT = "SQL_FIND_MATCHED_SOLUTION_BOLT";
    private static final String SQL_INSERT_CHECKUP_ITEM_BOLT = "SQL_INSERT_CHECKUP_ITEM_BOLT";
    private static final String SQL_UPDATE_USERRULE_STATUS_BOLT = "SQL_UPDATE_USERRULE_STATUS_BOLT";
    private static final String SQL_UPDATE_LAST_GENERATED_TIME = "SQL_UPDATE_LAST_GENERATED_TIME";
    
    private static final String SQL_FIND_MATCHED_USERRULE="select rule_id,user_id,user_id as checkupitempidrefix,guideline_id,originate,description,concernedFactors,riskDefine,disease_name,riskType from ta_userRule where user_id=? and status='match'";
    private static final String SQL_FIND_MATCHED_SOLUTION="select a.examsolution_id,concat(?,'-',a.examsolution_id) as checkupitem_id,a.subgroup,a.riskType,a.startage,a.endage,a.features,a.examguideline_id as guideline_id,? as user_id,? as originate,? as description,? as concernedFactors,? as riskDefine,? as disease_name,? as rule_id,b.name as frequency,c.name as examitem from exam_examsolution a left join exam_examfrequency b on b.examfrequency_id=a.examfrequency_id left join exam_examitem c on c.examitem_id=a.examitem_id where examguideline_id=? and riskType=?";
        
    public static void main(String[] args) throws Exception {
        new GenerateCheckupSolutionTopology().execute(args);
    }

    @Override
    public StormTopology getTopology() {
    	
    	//Stream: get stream data from Queue like Kafka
//    	UserSpout userSpout = new UserSpout();
     	UserSpout userSpout = new UserSpout(connectionProvider,"lastMatchedOn","lastEvaluatedOn","lastMatchedOn","lastPreparedOn");
    	
        //SQL:select all matched userRule
    	//select rule_id,guideline_id,riskType,user_id from ta_userRule where User_id="$user_id" and status="match"
    	String sql = prop.getProperty("mysql.query.matched.userRule", SQL_FIND_MATCHED_USERRULE);
        Fields outputFields = new Fields("rule_id","checkupitempidrefix","user_id","guideline_id","originate","description","concernedFactors",
        		"riskDefine","disease_name","riskType");//Here we query all userRule columns
        List<Column> queryParamColumns = Lists.newArrayList(new Column("user_id", Types.VARCHAR));
        JdbcLookupMapper jdbcLookupMapper = new SimpleJdbcLookupMapper(outputFields, queryParamColumns);
        JdbcLookupBolt jdbcFindMatchedUserRuleBolt = new JdbcLookupBolt(connectionProvider, sql, jdbcLookupMapper);
        
        //SQL:find matched exam solutions
        //select “$user_id"as user_id,“$rule_id" as rule_id,if(count(*)>0,“match",“dismatch") as status from ta_user where user_id=$user_id and $ruleExpression
    	String sqlFindMatchedExamSolution = prop.getProperty("mysql.query.matched.solution", SQL_FIND_MATCHED_SOLUTION);
        Fields matchedExamSolutionOutputFields = new Fields("examsolution_id","checkupitem_id","subgroup","riskType","startage","endage","features","user_id","guideline_id","originate","description","concernedFactors","riskDefine","disease_name","rule_id","frequency","examitem");//Here we get match results
        List<Column> matchedExamSolutionParams = Lists.newArrayList(
        		new Column("checkupitempidrefix", Types.VARCHAR),
        		new Column("user_id", Types.VARCHAR),
        		new Column("originate", Types.VARCHAR),
        		new Column("description", Types.VARCHAR),
        		new Column("concernedFactors", Types.VARCHAR),
        		new Column("riskDefine", Types.VARCHAR),
        		new Column("disease_name", Types.VARCHAR),
        		new Column("rule_id", Types.VARCHAR),
        		new Column("guideline_id", Types.VARCHAR),
        		new Column("riskType", Types.VARCHAR));
        JdbcLookupMapper jdbcUserRuleMatchMapper = new SimpleJdbcLookupMapper(matchedExamSolutionOutputFields, matchedExamSolutionParams);
        JdbcLookupBolt jdbcFindExamSolutionsBolt = new JdbcLookupBolt(connectionProvider, sqlFindMatchedExamSolution, jdbcUserRuleMatchMapper);
        

        //SQL:insert checkup items
        //
        List<Column> checkupItemSchemaColumns = Lists.newArrayList(
        		new Column("checkupitem_id", Types.VARCHAR),
        		new Column("subgroup", Types.VARCHAR),
        		new Column("examitem", Types.VARCHAR),
        		new Column("features", Types.VARCHAR),
        		new Column("frequency", Types.VARCHAR),
        		new Column("originate", Types.VARCHAR),
        		new Column("description", Types.VARCHAR),
        		new Column("user_id", Types.VARCHAR),
        		new Column("concernedFactors", Types.VARCHAR),
        		new Column("disease_name", Types.VARCHAR),
        		new Column("riskType", Types.VARCHAR),
        		new Column("examsolution_id", Types.VARCHAR),
        		new Column("riskDefine", Types.VARCHAR),
        		new Column("startage", Types.INTEGER),
        		new Column("endage", Types.INTEGER));//used for query values from tuple
        JdbcMapper checkupItemMapper = new SimpleJdbcMapper(checkupItemSchemaColumns);//define tuple columns
        JdbcInsertBolt jdbcInsertCheckupItemBolt = new JdbcInsertBolt(connectionProvider, checkupItemMapper)
                .withInsertQuery("insert into tb_checkupitem(checkupitem_id,subgroup,name,features,frequency,status,originate,description,generatedtime,"
                		+ "worker,revision,sysflag,checkuppackage_id,user_id,concernedFactors,disease_name,riskType,solution_id,riskDefine,startage,endage) "
                		+ "values (?,?,?,?,?,'ready',?,?,now(),'robot','1','pending',user_id,?,?,?,?,?,?,?,?) on duplicate key update revision=revision+1");
                
        //SQL:update UserRule status
        //update ta_userRule set status=“done” where user_id=“$user_id” and rule_id=“$rule_id”
        List<Column> userRuleSchemaColumns = Lists.newArrayList(new Column("user_id", Types.VARCHAR),
        		new Column("rule_id", Types.VARCHAR));//used for query values from tuple
        JdbcMapper userRuleMapper = new SimpleJdbcMapper(userRuleSchemaColumns);//define tuple columns
        JdbcInsertBolt updateUserRuleStatusBolt = new JdbcInsertBolt(connectionProvider, userRuleMapper)
                .withInsertQuery("update ta_userRule set status='done' where user_id=? and rule_id=?");
            
        //SQL: update lastGeneratedOn timestamp
        List<Column> timestampSchemaColumns = Lists.newArrayList(new Column("user_id", Types.VARCHAR));
        JdbcMapper timestampMapper = new SimpleJdbcMapper(timestampSchemaColumns);//define tuple columns
        JdbcInsertBolt jdbcUpdateUserTimestampBolt = new JdbcInsertBolt(connectionProvider, timestampMapper)
                .withInsertQuery("update ta_user set lastGeneratedOn=now(),status='generated' where user_id=?");
                
        //TOPO:userSpout ==> findNewUserBolt ==> insertCheckupPackageBolt
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(USER_SPOUT, userSpout, 1);//TODO here we should put candidate user in a queue like Kafka
        builder.setBolt(SQL_FIND_MATCHED_USERRULE_BOLT, jdbcFindMatchedUserRuleBolt, 1).shuffleGrouping(USER_SPOUT);
        builder.setBolt(SQL_FIND_MATCHED_SOLUTION_BOLT, jdbcFindExamSolutionsBolt, 1).shuffleGrouping(SQL_FIND_MATCHED_USERRULE_BOLT);
        builder.setBolt(SQL_UPDATE_LAST_GENERATED_TIME, jdbcUpdateUserTimestampBolt, 1).shuffleGrouping(SQL_FIND_MATCHED_SOLUTION_BOLT);
        builder.setBolt(SQL_INSERT_CHECKUP_ITEM_BOLT, jdbcInsertCheckupItemBolt, 1).shuffleGrouping(SQL_FIND_MATCHED_SOLUTION_BOLT);
        builder.setBolt(SQL_UPDATE_USERRULE_STATUS_BOLT, updateUserRuleStatusBolt,1).shuffleGrouping(SQL_FIND_MATCHED_SOLUTION_BOLT);
        return builder.createTopology();
    }
}
