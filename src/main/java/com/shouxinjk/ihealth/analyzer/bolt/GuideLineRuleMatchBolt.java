package com.shouxinjk.ihealth.analyzer.bolt;

import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.commons.lang.Validate;
import org.apache.storm.jdbc.bolt.AbstractJdbcBolt;
import org.apache.storm.jdbc.common.Column;
import org.apache.storm.jdbc.common.ConnectionProvider;
import org.apache.storm.jdbc.mapper.JdbcLookupMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

/**
 * Basic bolt for querying from any database.
 */
public class GuideLineRuleMatchBolt extends AbstractJdbcBolt {
    private static final Logger LOG = LoggerFactory.getLogger(GuideLineRuleMatchBolt.class);
    public List<Column> columns;
    private String selectQuery;

    private JdbcLookupMapper jdbcLookupMapper;

    public GuideLineRuleMatchBolt(ConnectionProvider connectionProvider, String selectQuery, JdbcLookupMapper jdbcLookupMapper) {
        super(connectionProvider);

        Validate.notNull(selectQuery);
        Validate.notNull(jdbcLookupMapper);

        this.selectQuery = selectQuery;
        this.jdbcLookupMapper = jdbcLookupMapper;
        this.columns = new ArrayList<Column>();
    }

    public GuideLineRuleMatchBolt withQueryTimeoutSecs(int queryTimeoutSecs) {
        this.queryTimeoutSecs = queryTimeoutSecs;
        return this;
    }

    //rule_id,user_id,ruleExpression
    public void execute(Tuple tuple) {
        try {
            List<Column> columns = jdbcLookupMapper.getColumns(tuple);
            //prepare query parameters:just remove the last elements
            columns.remove(3);
            //modify query SQL
            String conditionSQL = tuple.getString(3);
            String sql = this.selectQuery+ " and "+conditionSQL;
            LOG.debug("start match user rule.[SQL]"+sql+"[user_id]"+tuple.getString(0)+"[rule_id]"+tuple.getString(1)+"[user_id2]"+tuple.getString(2));
            List<List<Column>> result = jdbcClient.select(sql, columns);

            if (result != null && result.size() != 0) {
                for (List<Column> row : result) {
                    List<Values> values = jdbcLookupMapper.toTuple(tuple, row);
                    for (Values value : values) {
                        collector.emit(tuple, value);
                    }
                }
            }
            this.collector.ack(tuple);
        } catch (Exception e) {
            this.collector.reportError(e);
            this.collector.fail(tuple);
        }
    }

    public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
    	//"user_id","rule_id","status"
        jdbcLookupMapper.declareOutputFields(outputFieldsDeclarer);
//        outputFieldsDeclarer.declare(new Fields("user_id","rule_id","status"));
    }
}
