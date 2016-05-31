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
import com.shouxinjk.ihealth.analyzer.spout.PendingCheckupItemSpout;
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


public class CleanTopology extends AbstractCheckupSolutionTopology {
    private static final String PENDING_CHECKUP_ITEM_SPOUT = "PENDING_CHECKUP_ITEM_SPOUT";
    private static final String SQL_DELETE_UNMATCH_CHECKUP_ITEM = "SQL_DELETE_UNMATCH_CHECKUP_ITEM";

    public static void main(String[] args) throws Exception {
        new CleanTopology().execute(args);
    }

    @Override
    public StormTopology getTopology() {
    	  	
    	PendingCheckupItemSpout pendingCheckupItemSpout = new PendingCheckupItemSpout(connectionProvider);
    	    	
        //SQL:delete umatched items
        //delete from tb_checkupitem where status!='已选中' and sysflag='pending' and checkupitem_id=$checkupitem_id and (startage>? or endage<?) 
        List<Column> schemaColumns = Lists.newArrayList(new Column("checkupitem_id", Types.VARCHAR),
        		new Column("age", Types.INTEGER),
        		new Column("age2", Types.INTEGER));//used for query values from tuple
        JdbcMapper mapper = new SimpleJdbcMapper(schemaColumns);//define tuple columns
        JdbcInsertBolt jdbcCleanCheckupItemBolt = new JdbcInsertBolt(connectionProvider, mapper)
                .withInsertQuery("delete from tb_checkupitem where status!='已选中' and checkupitem_id=? and (startage>? or endage<?)");
 
        TopologyBuilder builder = new TopologyBuilder();
        builder.setSpout(PENDING_CHECKUP_ITEM_SPOUT, pendingCheckupItemSpout, 1);//here we load all pending checkup items
        builder.setBolt(SQL_DELETE_UNMATCH_CHECKUP_ITEM, jdbcCleanCheckupItemBolt, 5).shuffleGrouping(PENDING_CHECKUP_ITEM_SPOUT);
        return builder.createTopology();
    }
}
