package newborn_town.pspm.online.topology;

import newborn_town.constant.StormConfig;
import newborn_town.pspm.online.bolt.PSPMConversionToHBase;

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
 * @author  yujiwen E-mail: yujiwen@newborn-town.com
 * @version 创建时间：2016年12月9日 下午6:53:26
 * 
 */
public class PSPMConversionLogToHBaseTopology {


	public static void run(String str) {

		KafkaSpout kafkaSpout = StormConfig
				.getKafkaStormConfigurationAllPartition("pspmConversionToHBase", "/topics",
						"__pspmConversionHBase_offset");

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", kafkaSpout, 1);
		
		builder.setBolt("pspmConversionToHBaseBolt", new PSPMConversionToHBase(),
				1).shuffleGrouping("spout");

		
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
				cluster.submitTopology("pspmConversionToHBase", conf,
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
				.getKafkaStormConfigurationAllPartition("pspmConversionToHBase", "/topics",
						"__pspmConversionHBase_offset");

		TopologyBuilder builder = new TopologyBuilder();

		builder.setSpout("spout", kafkaSpout, 1);
		
		builder.setBolt("pspmConversionToHBaseBolt", new PSPMConversionToHBase(),
				1).shuffleGrouping("spout");

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
				cluster.submitTopology("pspmConversionToHBase", conf,
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
