package org.anirudh.redquark.stormstarter.bolt;

import java.util.Map;

import org.apache.storm.task.OutputCollector;
import org.apache.storm.task.TopologyContext;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseRichBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;

public class PrimeNumberBolt extends BaseRichBolt {

	/**
	 * Default generated serialVersionUID
	 */
	private static final long serialVersionUID = 1L;

	// Creating the collector instance
	private OutputCollector collector;

	public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

		this.collector = collector;
	}

	public void execute(Tuple input) {

		int number = input.getInteger(0);

		if (isPrime(number)) {
			System.out.println(number);
		}

		collector.ack(input);

	}

	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		
		declarer.declare(new Fields("number"));

	}

	private boolean isPrime(int n) {
		if (n == 1 || n == 2 || n == 3) {
			return true;
		}

		// Is n an even number?
		if (n % 2 == 0) {
			return false;
		}

		// if not, then just check the odds
		for (int i = 3; i * i <= n; i += 2) {
			if (n % i == 0) {
				return false;
			}
		}
		return true;
	}

}
