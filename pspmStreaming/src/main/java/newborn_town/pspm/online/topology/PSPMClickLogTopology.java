package newborn_town.pspm.online.topology;

import newborn_town.constant.StormConfig;
import newborn_town.pspm.online.bolt.PSPMClickToApiReportBolt;
import newborn_town.pspm.online.bolt.PSPMClickToBaseReportBolt;
import newborn_town.pspm.online.bolt.PSPMClickToCampaignReportBolt;
import newborn_town.pspm.online.bolt.PSPMClickToCountryReportBolt;
import newborn_town.pspm.online.bolt.PSPMClickLogGroupingBolt;
import newborn_town.pspm.online.bolt.PSPMClickToHBase;

import org.apache.commons.lang.StringUtils;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.tuple.Fields;
import org.apache.storm.utils.Utils;

/**
 * 实时消费kafka中pspm click日志并插入到mogondb
 * 
 * @author yangyang
 *
 */
public class PSPMClickLogTopology {

	public static void run(String str) {
		KafkaSpout kafkaSpout = StormConfig
				.getKafkaStormConfigurationAllPartition("pspmClickToMongoAndHBase", "/topics",
						"__pspmClickToMongoAndHBase_offset");

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", kafkaSpout, 3);
		
		builder.setBolt("pspmClickToHBaseBolt", new PSPMClickToHBase(), 6)
		.shuffleGrouping("spout");// 设置根据规则分发日志bolt
		

		builder.setBolt("group", new PSPMClickLogGroupingBolt(), 3)
				.shuffleGrouping("spout");// 设置根据规则分发日志bolt

		builder.setBolt("baseReport", new PSPMClickToBaseReportBolt(), 1)
				.fieldsGrouping("group", new Fields("clickgroupingStr"));

		builder.setBolt("campaignReport",
				new PSPMClickToCampaignReportBolt(), 1).fieldsGrouping(
				"group", new Fields("clickgroupingStr"));

		builder.setBolt("countryReport", new PSPMClickToCountryReportBolt(),
				1).fieldsGrouping("group", new Fields("clickgroupingStr"));

		builder.setBolt("apiReport", new PSPMClickToApiReportBolt(), 1)
				.fieldsGrouping("group", new Fields("clickgroupingStr"));


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
				cluster.submitTopology("pspmClickToMongoAndHBase", conf,
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
				.getKafkaStormConfigurationAllPartition("pspmClickToMongoAndHBase", "/topics",
						"__pspmClickToMongoAndHBase_offset");

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", kafkaSpout, 3);
		
		builder.setBolt("pspmClickToHBaseBolt", new PSPMClickToHBase(), 3)
		.shuffleGrouping("spout");// 设置根据规则分发日志bolt

		builder.setBolt("group", new PSPMClickLogGroupingBolt(), 3)
				.shuffleGrouping("spout");// 设置根据规则分发日志bolt

		builder.setBolt("baseReport", new PSPMClickToBaseReportBolt(), 1)
				.fieldsGrouping("group", new Fields("clickgroupingStr"));

		builder.setBolt("campaignReport",
				new PSPMClickToCampaignReportBolt(), 1).fieldsGrouping(
				"group", new Fields("clickgroupingStr"));

		builder.setBolt("countryReport", new PSPMClickToCountryReportBolt(),
				1).fieldsGrouping("group", new Fields("clickgroupingStr"));

		builder.setBolt("apiReport", new PSPMClickToApiReportBolt(), 1)
				.fieldsGrouping("group", new Fields("clickgroupingStr"));

	
		Config conf = new Config();
		conf.setDebug(false);

		try {

			if (args != null && args.length > 0) {
				conf.setNumWorkers(1);
				StormSubmitter.submitTopology(args[0], conf,
						builder.createTopology());
			} else {
				conf.setMaxTaskParallelism(3);
				LocalCluster cluster = new LocalCluster();
				cluster.submitTopology("pspmClickToMongoAndHBase", conf,
						builder.createTopology());
				/*
				 * StormSubmitter.submitTopology("clicksToMogondbOnline", conf,
				 * builder.createTopology());
				 */
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
