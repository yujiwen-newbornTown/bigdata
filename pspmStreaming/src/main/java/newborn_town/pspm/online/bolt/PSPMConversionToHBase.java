package newborn_town.pspm.online.bolt;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import newborn_town.util.HbaseUtil;
import newborn_town.util.HttpUtil;
import newborn_town.util.PublicUtill;

import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.NameValuePair;
import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;

/**
 * @author  yujiwen E-mail: yujiwen@newborn-town.com
 * @version 创建时间：2016年12月9日 下午1:03:40
 * 
 */
public class PSPMConversionToHBase extends BaseBasicBolt {


	private static final long serialVersionUID = -3741400208496719631L;

	static Logger logger = Logger
			.getLogger(PSPMConversionToHBase.class);

	static String tableName = "uuid_match";// 表名 
	static byte[] family = Bytes.toBytes("info"); // 列族
	
	static int HBASE_NUM = 1000; // 批量存储Mogondb的阈值
	static int secondNum = 10;

	HttpUtil httpUtil = null;
	HbaseUtil hbaseClient = null;
	byte[] postByte = Bytes.toBytes("conversionlog");
	byte[] clickByte = Bytes.toBytes("clicklog");
	
	String postUrl = "http://pspm.pingstart.com/api/v4/postback_callback";
	
	HashMap<String,String> uuidLogMap = new HashMap<String,String>();
	HashMap<String,HashMap<String,String>> updateMap = new HashMap<String,HashMap<String,String>>();
	
	/*
	 * conversion log format 0click_id | 1now_time | 2campaign_id | 3click_geo |
	 * 4platform | 5category | 6package | 7publisher_id | 8publisher_slot |
	 * 9publisher_type | 10am | 11advertiser_id | 12adver_type | 13bd | 14pm |
	 * 15weget | 16payout | 17sub_1 | 18sub_2 | 19sub_3 | 20con_ua | 21con_ip |
	 * 22con_referrer | 23con_gaid | 24con_aid | 25con_idfa | 26con_geo |
	 * 27con_time | 28con_Osversion | 29device_brand | 30device_carrier |
	 * 31device_type | 32device_language | 33match_type | 34google_ad_tracking |
	 * 35package_app_version | 36status
	 * 
	 * @see
	 * org.apache.storm.topology.IBasicBolt#execute(org.apache.storm.tuple.Tuple
	 * , org.apache.storm.topology.BasicOutputCollector)
	 */

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

			// 定时插入HBase
			if (uuidLogMap.size() > 0 ) {
				updateDB();
			}

			return;
		}

		String conversionlog = input.getString(0).substring(3);

		String[] sentence = conversionlog.split("\\|");
		if(sentence == null || sentence.length==0){
			return;
		}
		
		HashMap<String,String> inMap = new HashMap<String,String>();
		inMap.put("conversionlog", conversionlog);
		String uuid = sentence[0];
		uuidLogMap.put(uuid, conversionlog);
		updateMap.put(uuid, inMap);
	
		// 判断click的时间是否为一分钟之内或者updateMap数据大于1000条进行HBase存储
		if (uuidLogMap.size() > HBASE_NUM) {
			updateDB();
		}
	}
	
	private void updateDB(){
		ArrayList<byte[]> uuidList = PublicUtill.listToBytes(uuidLogMap.keySet());
		Result[] rs = hbaseClient.getRowsRecord(tableName, uuidList);
		for(Result r : rs){
			if(!r.containsColumn(family, postByte) && r.containsColumn(family, clickByte)){
				String click = Bytes.toString(r.getValue(family, clickByte));
				String postback = uuidLogMap.get(Bytes.toString(r.getRow()));
				List<NameValuePair> namepairList = new ArrayList<NameValuePair>();
			
				namepairList.add(new BasicNameValuePair("click_info",click));
				namepairList.add(new BasicNameValuePair("postback_info",postback));
				String result = HttpUtil.httpPost(postUrl, namepairList, 5000);
				
				if(result!=null){		        
					hbaseClient.insertData(tableName, r.getRow(), family, postByte, Bytes.toBytes(postback));
					logger.info(postback);
				}
			}
			
		}
			
		uuidLogMap.clear();
		updateMap.clear();

	}

//	private void insterOrUpdateDB(HashMap<String,HashMap<String,String>> updateMap) {
//
//		HashMap<byte[], HashMap<byte[], byte[]>> rcvalues = PublicUtill.mapStringToBytes(updateMap);
//		hbaseClient.insertDatas(tableName, family, rcvalues);
//		
//		logger.info("PSPMConversionToHBase 插入或更新"
//				+ updateMap.size() + "条数据！");
//
//	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {

	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Config config = new Config();
		config.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, secondNum);
		return config;
	}
	
	
}
