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

import org.apache.storm.Config;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.StormTopology;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.common.HikariCPConnectionProvider;
import org.apache.storm.jdbc.common.JdbcClient;
import org.apache.storm.jdbc.mapper.JdbcMapper;
import org.apache.storm.jdbc.mapper.JdbcLookupMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcMapper;
import org.apache.storm.jdbc.mapper.SimpleJdbcLookupMapper;
import org.apache.storm.LocalCluster;

import java.sql.Types;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public abstract class AbstractCheckupSolutionTopology {

    protected ConnectionProvider connectionProvider;
    protected Properties prop=new Properties();

    protected static final String JDBC_CONF = "jdbc.conf";
    
    public void execute(String[] args) throws Exception {
    	//here we load configurations from properties file
        prop.load(AbstractCheckupSolutionTopology.class.getClassLoader().getResourceAsStream("ihealth-analyzer.properties"));
        
        //prepare storm configuration
        Config config = new Config();
        if("debug".equalsIgnoreCase(prop.getProperty("common.mode", "production")))
        	config.setDebug(true); 
        else
        	config.setDebug(false);
        
        //prepare JDBC configuration
        Map jdbcConfigMap = Maps.newHashMap();
        jdbcConfigMap.put("dataSourceClassName", prop.getProperty("mysql.dataSource.className"));//com.mysql.jdbc.jdbc2.optional.MysqlDataSource
        jdbcConfigMap.put("dataSource.url", prop.getProperty("mysql.url"));//jdbc:mysql://localhost/test
        jdbcConfigMap.put("dataSource.user", prop.getProperty("mysql.user"));//root
        jdbcConfigMap.put("dataSource.password", prop.getProperty("mysql.password"));//password
        config.put(JDBC_CONF, jdbcConfigMap);  
        this.connectionProvider = new HikariCPConnectionProvider(jdbcConfigMap);
        
        //prepare CASSANDRA configuration
        config.put("cassandra.keyspace",prop.getProperty("cassandra.keyspace","mykeyspace"));	
        config.put("cassandra.nodes",prop.getProperty("cassandra.nodes","localhost"));
        config.put("cassandra.username",prop.getProperty("cassandra.username",""));
        config.put("cassandra.password",prop.getProperty("cassandra.password",""));
        config.put("cassandra.port",prop.getProperty("cassandra.port","9042"));
        config.put("cassandra.output.consistencyLevel",prop.getProperty("cassandra.output.consistencyLevel","ONE"));
        config.put("cassandra.batch.size.rows",prop.getProperty("cassandra.batch.size.rows","100"));
        config.put("cassandra.retryPolicy",prop.getProperty("cassandra.retryPolicy","DefaultRetryPolicy"));
        config.put("cassandra.reconnectionPolicy.baseDelayMs",prop.getProperty("cassandra.reconnectionPolicy.baseDelayMs","100"));
        config.put("cassandra.reconnectionPolicy.maxDelayMs",prop.getProperty("cassandra.reconnectionPolicy.maxDelayMs","60000"));

        config.setNumAckers(0);//we don't need ack
//        System.out.println(config);
        
        //submit topology
        String topoName = prop.getProperty("common.topology.name", "ihealthAnalyzeTopology");
        if (args != null && args.length > 0) {
        	config.setNumWorkers(1); 
            StormSubmitter.submitTopology(args[0], config, getTopology());
        } else {
        	//TODO to set TOPOLOGY_ACKERS to 0 to disable ack
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(topoName, config, getTopology());
            Thread.sleep(30000);
            cluster.killTopology(topoName);
            cluster.shutdown();
        }
    }

    public abstract StormTopology getTopology();

}
