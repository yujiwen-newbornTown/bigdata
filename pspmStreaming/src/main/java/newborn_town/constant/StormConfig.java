package newborn_town.constant;

import java.util.ArrayList;
import java.util.List;

import org.apache.storm.kafka.Broker;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.kafka.SpoutConfig;
import org.apache.storm.kafka.StaticHosts;
import org.apache.storm.kafka.StringScheme;
import org.apache.storm.kafka.ZkHosts;
import org.apache.storm.kafka.trident.GlobalPartitionInformation;
import org.apache.storm.spout.SchemeAsMultiScheme;

/**
 * @author yangyang
 * @version 创建时间：2016年8月2日 上午9:52:07
 */
public class StormConfig {

	/**
	 * 获取KafkaSpout配置(消费某个分区)
	 * @param topicName topic名称 
	 * @param topicPath offerset的zk路径
	 * @param offerSetsPath offerset的保存路径
	 * @return
	 */
	public static KafkaSpout getKafkaStormConfigurationAllPartition(String topicName,
			String topicPath, String offerSetsPath) {

		String zkConnString = Constant.ZKHosts;

		ZkHosts zkHosts = new ZkHosts(zkConnString);

		SpoutConfig kafkaConfig = new SpoutConfig(zkHosts, topicName, topicPath,
				offerSetsPath);

		long startOffsetTime = kafka.api.OffsetRequest.LatestTime();

		List<String> zkServers = new ArrayList<String>();

		for (String host : zkHosts.brokerZkStr.split(",")) {
			zkServers.add(host.split(":")[0]);
		}

		kafkaConfig.zkServers = zkServers;
		kafkaConfig.ignoreZkOffsets = false;//是否忽略zk中offsets的位置
		kafkaConfig.zkPort = 2181;
		kafkaConfig.startOffsetTime = startOffsetTime;// 设置消费最新的数据

		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);

		return kafkaSpout;
	}

	/**
	 * 获取KafkaSpout配置(消费某个分区)
	 * @param topicName  topic名称 
	 * @param topicPath  offerset的zk路径
	 * @param offerSetsPath offerset的保存路径
	 * @return
	 */
	public static KafkaSpout getKafkaStormConfigurationSomePartition(String topicName,
			String topicPath, String offerSetsPath) {

		String zkConnString = Constant.ZKHosts;

		ZkHosts zkHosts = new ZkHosts(zkConnString);

		GlobalPartitionInformation partitionInfo = new GlobalPartitionInformation(
				zkConnString);

		Broker brokerForPartition = new Broker(Constant.KAFKA_HOST);

		partitionInfo.addPartition(1, brokerForPartition);

		StaticHosts hosts = new StaticHosts(partitionInfo);

		SpoutConfig kafkaConfig = new SpoutConfig(hosts, topicName, topicPath,
				offerSetsPath);

		long startOffsetTime = kafka.api.OffsetRequest.LatestTime();

		List<String> zkServers = new ArrayList<String>();

		for (String host : zkHosts.brokerZkStr.split(",")) {
			zkServers.add(host.split(":")[0]);
		}

		kafkaConfig.zkServers = zkServers;
		kafkaConfig.ignoreZkOffsets = true;
		kafkaConfig.zkPort = 2181;
		kafkaConfig.startOffsetTime = startOffsetTime;// 设置消费最新的数据

		kafkaConfig.scheme = new SchemeAsMultiScheme(new StringScheme());
		KafkaSpout kafkaSpout = new KafkaSpout(kafkaConfig);

		return kafkaSpout;
	}

}
