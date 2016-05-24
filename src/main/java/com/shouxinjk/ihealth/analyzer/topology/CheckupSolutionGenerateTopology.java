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

/**
 * @deprrecated
 * 
 * this is only a sample
 */

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


public class CheckupSolutionGenerateTopology extends AbstractCheckupSolutionTopology {
    private static final String USER_SPOUT = "USER_SPOUT";
    private static final String SQL_FIND_NEW_USER_BOLT = "FIND_NEW_USER_BOLT";
    private static final String SQL_INSERT_CHECKUP_PACKAGE_BOLT = "INSERT_CHECKUP_PACKAGE_BOLT";
    
    private static final String SQL_FIND_NEW_USER="select user_id from sys_app_user where user_id not in (select user_id from tb_checkuppackage)";

    public static void main(String[] args) throws Exception {
        new CheckupSolutionGenerateTopology().execute(args);
    }

    @Override
    public StormTopology getTopology() {
    	
    	//Stream: get stream data from Queue like Kafka
    	UserSpout userSpout = new UserSpout(null);
    	
        //SQL:select all users that don't have checkup package
    	String sql = prop.getProperty("mysql.query.newuser", SQL_FIND_NEW_USER);
        Fields outputFields = new Fields("user_id","checkuppackage_id");//Here we query all users that don't have checkup package
        List<Column> queryParamColumns = Lists.newArrayList(new Column("user_id", Types.VARCHAR));
        JdbcLookupMapper jdbcLookupMapper = new SimpleJdbcLookupMapper(outputFields, queryParamColumns);
        JdbcLookupBolt jdbcFindNewUserBolt = new JdbcLookupBolt(connectionProvider, sql, jdbcLookupMapper);

        //SQL:insert blank checkup package
        List<Column> schemaColumns = Lists.newArrayList(new Column("user_id", Types.VARCHAR),new Column("checkuppackage_id", Types.VARCHAR));//used for query values from tuple
        JdbcMapper mapper = new SimpleJdbcMapper(schemaColumns);//define tuple columns
        JdbcInsertBolt jdbcInsertCheckupPackageBolt = new JdbcInsertBolt(connectionProvider, mapper)
                .withInsertQuery("insert ignore into tb_checkuppackage (CHECKUPPACKAGE_ID, GENERATEDTIME, STATUS, REVISION,SYSFLAG,USER_ID) values (?,now(),'pending','0','inprocess',?)");
                
        
        //CQL:select detailed user info
//        CassandraWriterBolt cqlLoadUserDetailBolt = new CassandraWriterBolt(
//                async(
//                    simpleQuery("INSERT INTO users (user_id,  fname, lname) VALUES (?, ?, ?);")
//                        .with( all() )
//                    )
//            );
        //
        //TOPO:userSpout ==> findNewUserBolt ==> insertCheckupPackageBolt
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(USER_SPOUT, userSpout, 1);//TODO here we should put candidate user in a queue like Kafka
        builder.setBolt(SQL_FIND_NEW_USER_BOLT, jdbcFindNewUserBolt, 1).shuffleGrouping(USER_SPOUT);
        builder.setBolt(SQL_INSERT_CHECKUP_PACKAGE_BOLT, jdbcInsertCheckupPackageBolt, 1).shuffleGrouping(SQL_FIND_NEW_USER_BOLT);
        return builder.createTopology();
    }
}
