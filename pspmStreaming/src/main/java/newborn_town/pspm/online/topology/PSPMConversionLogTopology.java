package newborn_town.pspm.online.topology;

import newborn_town.constant.StormConfig;
import newborn_town.pspm.online.bolt.PSPMConversionToApiReportBolt;
import newborn_town.pspm.online.bolt.PSPMConversionToBaseReportBolt;
import newborn_town.pspm.online.bolt.PSPMConversionToCampaignReportBolt;
import newborn_town.pspm.online.bolt.PSPMConversionToCountryReportBolt;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.utils.Utils;

/**
 * 实时消费kafka中pspm conversion日志并插入到mogondb
 * 
 * @author yangyang
 *
 */
public class PSPMConversionLogTopology {

	public static void run(String str) {

		KafkaSpout kafkaSpout = StormConfig
				.getKafkaStormConfigurationAllPartition("pspmConversionToMongo", "/topics",
						"__pspmConversionToMongo_offset");

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", kafkaSpout, 1);

		builder.setBolt("baseReport", new PSPMConversionToBaseReportBolt(),
				1).shuffleGrouping("spout");

		builder.setBolt("campaignReport",
				new PSPMConversionToCampaignReportBolt(), 1)
				.shuffleGrouping("spout");

		builder.setBolt("countryReport",
				new PSPMConversionToCountryReportBolt(), 1).shuffleGrouping(
				"spout");
		
		builder.setBolt("apiReport",
				new PSPMConversionToApiReportBolt(), 1).shuffleGrouping(
				"spout");

		// builder.setBolt("pspmDay", new PSPMConversionDayToMogondbBolt(), 1)
		// .shuffleGrouping("spout");
		// builder.setBolt("pspmHour", new PSPMConversionHourToMogondbBolt(), 1)
		// .shuffleGrouping("spout");

		Config conf = new Config();
		conf.setDebug(false);

		try {

			if (StringUtils.isNotEmpty(str)) {
				conf.setNumWorkers(1);
				StormSubmitter.submitTopology(str, conf,
						builder.createTopology());
			} else {
				conf.setMaxTaskParallelism(3);
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("pspmConversionToMongo", conf,
						builder.createTopology());
				Utils.sleep(100000);
			}
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		} catch (AuthorizationException e) {
			e.printStackTrace();
		}
	}

	public static void main(String[] args) {

		KafkaSpout kafkaSpout = StormConfig
				.getKafkaStormConfigurationAllPartition("pspmConversionToMongo", "/topics",
						"__pspmConversionToMongo_offset");

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", kafkaSpout, 1);

		builder.setBolt("baseReport", new PSPMConversionToBaseReportBolt(),
				1).shuffleGrouping("spout");

		builder.setBolt("campaignReport",
				new PSPMConversionToCampaignReportBolt(), 1)
				.shuffleGrouping("spout");

		builder.setBolt("countryReport",
				new PSPMConversionToCountryReportBolt(), 1).shuffleGrouping(
				"spout");
		
		builder.setBolt("apiReport",
				new PSPMConversionToApiReportBolt(), 1).shuffleGrouping(
				"spout");

		// builder.setBolt("pspmDay", new PSPMConversionDayToMogondbBolt(), 1)
		// .shuffleGrouping("spout");
		// builder.setBolt("pspmHour", new PSPMConversionHourToMogondbBolt(), 1)
		// .shuffleGrouping("spout");

		Config conf = new Config();
		conf.setDebug(false);

		try {

			if (args != null && args.length > 0) {
				conf.setNumWorkers(3);
				StormSubmitter.submitTopology(args[0], conf,
						builder.createTopology());
			} else {
				conf.setMaxTaskParallelism(3);
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("pspmConversionToMongo", conf,
						builder.createTopology());
				Utils.sleep(100000);
			}
		} catch (AlreadyAliveException e) {
			e.printStackTrace();
		} catch (InvalidTopologyException e) {
			e.printStackTrace();
		} catch (AuthorizationException e) {
			e.printStackTrace();
		}
	}
}
