package org.anirudh.redquark.stormstarter.spout;

import java.util.Map;

import org.apache.storm.spout.SpoutOutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichSpout;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Values;

public class NumberSpout extends BaseRichSpout {

	/**
	 * Default generated serialVersionUID
	 */
	private static final long serialVersionUID = 1L;

	// Creating the collector instance
	private SpoutOutputCollector collector;

	// Creating the starting number
	private static int currentNumber = 1;

	public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {

		this.collector = collector;
	}

	public void nextTuple() {

		// Emit the next number
		collector.emit(new Values(new Integer(currentNumber++)));

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {

		declarer.declare(new Fields("number"));
	}

}
