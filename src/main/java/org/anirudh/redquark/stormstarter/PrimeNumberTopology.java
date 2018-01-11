package org.anirudh.redquark.stormstarter;

import org.anirudh.redquark.stormstarter.bolt.PrimeNumberBolt;
import org.anirudh.redquark.stormstarter.spout.NumberSpout;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * Hello world!
 *
 */
public class PrimeNumberTopology {

	public static void main(String[] args) {

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", new NumberSpout());
		builder.setBolt("prime", new PrimeNumberBolt()).shuffleGrouping("spout");

		Config config = new Config();

		LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("test", config, builder.createTopology());
		Utils.sleep(10000);
		cluster.killTopology("test");
		cluster.shutdown();

	}
}
