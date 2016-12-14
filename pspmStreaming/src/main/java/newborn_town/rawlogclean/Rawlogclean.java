package newborn_town.rawlogclean;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.Queue;

import newborn_town.constant.StormConfig;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.LocalCluster;
import org.apache.storm.StormSubmitter;
import org.apache.storm.generated.AlreadyAliveException;
import org.apache.storm.generated.AuthorizationException;
import org.apache.storm.generated.InvalidTopologyException;
import org.apache.storm.kafka.KafkaSpout;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.TopologyBuilder;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import org.apache.storm.utils.Utils;


/**
 * 对原始日志进行5分钟队列校验并发送到下一个bolt
 * Created by yangyang on 16/7/18.
 */

public class Rawlogclean {

	static Logger logger = Logger.getLogger(Rawlogclean.class);
	
	static Queue<TimeRecordBean> recordQueue = new LinkedList<TimeRecordBean>();
	
	static HashMap<String, String> recordMap = new HashMap<String, String>();
	
	public static class SplitSentence extends BaseBasicBolt {

		private static final long serialVersionUID = 2051998147764972409L;
		
		int count = 0;
		
		public void execute(Tuple input, BasicOutputCollector collector) {
			
			//TimeRecordBean timeRecord = null;
			
			String sentence = input.getString(0);

			if (StringUtils.isEmpty(sentence))
				return;
			
			String[] words = sentence.split("\\|");

			if (null == words || words.length < 28)
				return;

			//timeRecord = new TimeRecordBean(words[28],words[0]);
			
			count++;
			
			//5分钟队列验证 
			//if(RecordQueueManager.updateRecordsQueue(timeRecord, recordQueue, recordMap)){
				
				//验证通过则发送到下个bolt进行处理
				collector.emit(new Values(sentence));
				
				System.err.println(this + "---------" + count);
				
			///}
		}
		
		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			declarer.declare(new Fields("word"));
		}
	}

	
	/*public static class WordCount extends BaseBasicBolt {

		private static final long serialVersionUID = -4764350143452666863L;

		int count = 0;
		
		public void execute(Tuple input, BasicOutputCollector collector) {
			count ++;
			System.err.println("wordcount-------" + count);
		}

		public void declareOutputFields(OutputFieldsDeclarer declarer) {
			
		}
		
	}*/
	
	
	public static void main(String[] args) {

		KafkaSpout kafkaSpout = StormConfig.getKafkaStormConfigurationAllPartition("impression-topic","/topics","__impression_offsets");
		
		TopologyBuilder builder = new TopologyBuilder();
		
		// 设置Spout节点并分配并发数，该并发数将会控制该对象在集群中的线程数。
		builder.setSpout("spout", kafkaSpout, 1);
		
		// 设置数据处理节点并分配并发数。指定该节点接收Spout节点的策略为随机方式。
		builder.setBolt("logclean", new SplitSentence(), 1).shuffleGrouping(
				"spout");
		
		/*builder.setBolt("count", new WordCount(), 1).fieldsGrouping("logclean",
				new Fields("word"));*/

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
				cluster.submitTopology("kafkaStorm_yy", conf,
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
