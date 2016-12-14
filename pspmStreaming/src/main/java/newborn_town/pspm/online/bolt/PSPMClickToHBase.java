package newborn_town.pspm.online.bolt;

import java.util.HashMap;
import java.util.Map;

import newborn_town.util.HbaseUtil;
import newborn_town.util.PublicUtill;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 * @author  yujiwen E-mail: yujiwen@newborn-town.com
 * @version 创建时间：2016年12月9日 上午10:07:02
 * 
 */
public class PSPMClickToHBase extends BaseBasicBolt {

	
	private static final long serialVersionUID = -6707182554696236571L;
	static Logger logger = Logger.getLogger(PSPMClickToHBase.class);

	static String tableName = "uuid_match";// 表名 
	static byte[] family = Bytes.toBytes("info"); // 列族
	
	static int HBASE_NUM = 1000;
	static int secondNum = 10;

	HbaseUtil hbaseClient = null;
	
	HashMap<String,HashMap<String,String>> updateMap = new HashMap<String,HashMap<String,String>>();
	public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
			org.apache.storm.task.TopologyContext context) {
		hbaseClient = new HbaseUtil();
			
	};

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		// 判断是否是定时任务的消息
		if (input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
				&& input.getSourceStreamId().equals(
						Constants.SYSTEM_TICK_STREAM_ID)) {

			if (updateMap.size() > 0) {

				insterOrUpdateDB(updateMap);
				updateMap.clear();
			}

			return;
		}

		String clickLog = input.getString(0).substring(3);
		String[] sentence = clickLog.split("\\|");

		String uuid = sentence[0];
		
		try {
			HashMap<String,String> inMap = new HashMap<String,String>();
			inMap.put("clicklog", clickLog);
			updateMap.put(uuid, inMap);

			if (updateMap.size() > HBASE_NUM) {

				insterOrUpdateDB(updateMap);
				updateMap.clear();
			}

		} catch (Exception e) {
			logger.error(e);
		}
	}

	private void insterOrUpdateDB(HashMap<String,HashMap<String,String>> updateMap) {

		HashMap<byte[], HashMap<byte[], byte[]>> rcvalues = PublicUtill.mapStringToBytes(updateMap);
		hbaseClient.insertDatas(tableName, family, rcvalues);
		
		logger.info("PSPMClickToHBase 插入" + updateMap.size()
				+ "条数据！");
		
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	// 设置定时任务
	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config config = new Config();
		config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, secondNum);
		return config;
	}

}
