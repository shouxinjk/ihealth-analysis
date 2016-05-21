package com.shouxinjk.ihealth.analyzer.spout;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;
import org.junit.Assert;

import java.util.Map;
import java.util.Random;
import java.util.concurrent.atomic.AtomicLong;

public class WeatherSpout extends BaseRichSpout {

    private SpoutOutputCollector spoutOutputCollector;

    private String stationID;

    private AtomicLong maxQueries;

    private AtomicLong acks = new AtomicLong(0);

    private AtomicLong emit = new AtomicLong(0);

    /**
     * Creates a new {@link WeatherSpout} instance.
     * @param stationID The station ID.
     */
    public WeatherSpout(String stationID, int maxQueries) {
        this.stationID = stationID;
        this.maxQueries = new AtomicLong(maxQueries);
    }

    @Override
    public void ack(Object msgId) {
        acks.incrementAndGet();
    }

    @Override
    public void fail(Object msgId) {
        Assert.fail("Must never get fail tuple : " + msgId);
    }

    @Override
    public void close() {
        Assert.assertEquals(acks.get(), emit.get());
    }

	public void nextTuple() {
        if (emit.get() < maxQueries.get()) {
            spoutOutputCollector.emit(new Values(new Random().nextInt(10000),stationID, "name"+System.currentTimeMillis()), emit.incrementAndGet());
        }
	}


	public void open(Map arg0, TopologyContext arg1, SpoutOutputCollector collector) {
        this.spoutOutputCollector = collector;
	}


	public void declareOutputFields(OutputFieldsDeclarer outputFieldsDeclarer) {
		outputFieldsDeclarer.declare(new Fields("user_id", "fname", "lname"));
	}
}
