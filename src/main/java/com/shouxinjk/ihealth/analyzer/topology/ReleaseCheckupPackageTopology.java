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
public class ReleaseCheckupPackageTopology extends AbstractCheckupSolutionTopology {
    private static final String USER_SPOUT = "USER_SPOUT";
    private static final String SQL_FIND_READY_CHECKUP_PACKAGE = "SQL_FIND_READY_CHECKUP_PACKAGE";
    private static final String SQL_UPDATE_CHECKUP_PACKAGE_STATUS = "SQL_UPDATE_CHECKUP_PACKAGE_STATUS";
    private static final String SQL_UPDATE_LAST_EVALUATED_TIME = "SQL_UPDATE_LAST_EVALUATED_TIME";
    private static final String SQL_UPDATE_CHECKITEM_PACKAGE_ID = "SQL_UPDATE_CHECKITEM_PACKAGE_ID";
            
    public static void main(String[] args) throws Exception {
        new ReleaseCheckupPackageTopology().execute(args);
    }

    @Override
    public StormTopology getTopology() {
    	
    	//Stream: get stream data from Queue like Kafka
//    	UserSpout userSpout = new UserSpout();
     	UserSpout userSpout = new UserSpout(connectionProvider,"lastGeneratedOn","lastEvaluatedOn");
        //SQL:select pending checkup package info
    	//
    	String sql = "select ? as checkupPackage_id,if(count(user_id)>0,'inprocess','ready') as status from ta_userRule where user_id=? and status='match'";
        Fields outputFields = new Fields("user_id","checkupPackage_id","status");//Here we query statistic status against specified USER_ID
        List<Column> queryParamColumns = Lists.newArrayList(new Column("checkuppackage_id", Types.VARCHAR),new Column("user_id", Types.VARCHAR));
        JdbcLookupMapper jdbcLookupMapper = new SimpleJdbcLookupMapper(outputFields, queryParamColumns);
        JdbcLookupBolt jdbcFindReadyCheckupPackageBolt = new JdbcLookupBolt(connectionProvider, sql, jdbcLookupMapper);
      

        //SQL:update checkup package status
        //
        List<Column> checkupPcakgeSchemaColumns = Lists.newArrayList(
        		new Column("status", Types.VARCHAR),
        		new Column("checkupPackage_id", Types.VARCHAR));//used for query values from tuple
        JdbcMapper checkupPackageMapper = new SimpleJdbcMapper(checkupPcakgeSchemaColumns);//define tuple columns
        JdbcInsertBolt jdbcUpdateCheckupPackageStatusBolt = new JdbcInsertBolt(connectionProvider, checkupPackageMapper)
                .withInsertQuery("update tb_checkuppackage set status=?,generatedtime=now() where checkuppackage_id=?");
                
        //SQL: update lastGeneratedOn timestamp
        List<Column> timestampSchemaColumns = Lists.newArrayList(new Column("user_id", Types.VARCHAR));
        JdbcMapper timestampMapper = new SimpleJdbcMapper(timestampSchemaColumns);//define tuple columns
        JdbcInsertBolt jdbcUpdateUserTimestampBolt = new JdbcInsertBolt(connectionProvider, timestampMapper)
                .withInsertQuery("update ta_user set lastEvaluatedOn=now() where user_id=?");
        
        //SQL:update TA_USER last evaluated time
        //update 
        List<Column> checkupItemSchemaColumns = Lists.newArrayList(new Column("user_id", Types.VARCHAR));//used for query values from tuple
        JdbcMapper checkupItemMapper = new SimpleJdbcMapper(checkupItemSchemaColumns);//define tuple columns
        JdbcInsertBolt updateCheckupItemPackageIDBolt = new JdbcInsertBolt(connectionProvider, checkupItemMapper)
                .withInsertQuery("update tb_checkupitem set checkuppackage_id=user_id,sysflag='done' where user_id=?");
                               
                
        //TOPO:userSpout ==> findNewUserBolt ==> insertCheckupPackageBolt
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(USER_SPOUT, userSpout, 1);//TODO here we should put candidate user in a queue like Kafka
        builder.setBolt(SQL_FIND_READY_CHECKUP_PACKAGE, jdbcFindReadyCheckupPackageBolt, 1).shuffleGrouping(USER_SPOUT);
        builder.setBolt(SQL_UPDATE_LAST_EVALUATED_TIME, jdbcUpdateUserTimestampBolt, 1).shuffleGrouping(SQL_FIND_READY_CHECKUP_PACKAGE);
        builder.setBolt(SQL_UPDATE_CHECKUP_PACKAGE_STATUS, jdbcUpdateCheckupPackageStatusBolt, 1).shuffleGrouping(SQL_FIND_READY_CHECKUP_PACKAGE);
        builder.setBolt(SQL_UPDATE_CHECKITEM_PACKAGE_ID, updateCheckupItemPackageIDBolt,1).shuffleGrouping(USER_SPOUT);
        return builder.createTopology();
    }
}
