package com.shouxinjk.ihealth.analyzer.topology;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.cassandra.bolt.CassandraWriterBolt;
import org.apache.storm.shade.org.eclipse.jetty.util.ajax.JSON;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

import com.shouxinjk.ihealth.analyzer.bolt.GuideLineRuleMatchBolt;
import com.shouxinjk.ihealth.analyzer.spout.GuideLineRuleSpout;
import com.shouxinjk.ihealth.analyzer.spout.WeatherSpout;
import static org.apache.storm.cassandra.DynamicStatementBuilder.*;

public class WeatherTopology {
    public static void main(String[] args) throws Exception {  
        TopologyBuilder builder = new TopologyBuilder();   
        builder.setSpout("spout", new WeatherSpout("Test", 1000));  
        CassandraWriterBolt bolt = new CassandraWriterBolt(
                async(
                    simpleQuery("INSERT INTO users (user_id,  fname, lname) VALUES (?, ?, ?);")
                        .with( all() )
                    )
            );
        builder.setBolt("bolt", bolt).shuffleGrouping("spout"); 
        Config conf = new Config();  

conf.put("cassandra.keyspace","mykeyspace");	
conf.put("cassandra.nodes","localhost");
conf.put("cassandra.username","");
conf.put("cassandra.password","");
conf.put("cassandra.port","9042");
conf.put("cassandra.output.consistencyLevel","ONE");
conf.put("cassandra.batch.size.rows","100");
conf.put("cassandra.retryPolicy","DefaultRetryPolicy");
conf.put("cassandra.reconnectionPolicy.baseDelayMs","100");
conf.put("cassandra.reconnectionPolicy.maxDelayMs","60000");

        conf.setDebug(false); 
        if (args != null && args.length > 0) {  
            conf.setNumWorkers(3);  
            StormSubmitter.submitTopology(args[0], conf, builder.createTopology());  
        } else {  
            LocalCluster cluster = new LocalCluster();  
            cluster.submitTopology("firstTopo", conf, builder.createTopology());  
            Utils.sleep(100000);  
            cluster.killTopology("firstTopo");  
            cluster.shutdown();  
        }  
    } 
}
