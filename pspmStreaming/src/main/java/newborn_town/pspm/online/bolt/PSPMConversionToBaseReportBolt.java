package newborn_town.pspm.online.bolt;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import newborn_town.util.DateUtil;
import newborn_town.util.MongoUtil;
import newborn_town.util.PublicUtill;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;
import org.apache.storm.Config;
import org.apache.storm.Constants;
import org.apache.storm.topology.BasicOutputCollector;
import org.apache.storm.topology.OutputFieldsDeclarer;
import org.apache.storm.topology.base.BaseBasicBolt;
import org.apache.storm.tuple.Tuple;
import org.bson.Document;

/**
 * 处理pspm Conversion原始日志并计算revenue,cost,profit 批量插入mogondb （天）
 * 
 * @author yangyang
 *
 */
public class PSPMConversionToBaseReportBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -3741400208496719631L;

	static Logger logger = Logger
			.getLogger(PSPMConversionToBaseReportBolt.class);

	static int MOGONDB_NUM = 5000; // 批量存储Mogondb的阈值

	int secondNum = 10;

	SimpleDateFormat timeDifferenceDF = null;

	MongoUtil mongoUtil = null;

	HashMap<String, Integer> logNumber = null;// log日志区分map

	ArrayList<Document> updateDocsStr = null;

	ArrayList<Document> updateDocsInc = null;

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
		timeDifferenceDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		mongoUtil = new MongoUtil("pspm", "baseReport");
		updateDocsInc = new ArrayList<Document>();
		updateDocsStr = new ArrayList<Document>();
		logNumber = new HashMap<String, Integer>();
	};

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		// 判断是否是定时任务的消息
		if (input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
				&& input.getSourceStreamId().equals(
						Constants.SYSTEM_TICK_STREAM_ID)) {

			// 定时插入mongo
			if (updateDocsInc.size() > 0 && updateDocsStr.size() > 0) {

				insterOrUpdateDB();

			}

			return;
		}

		String conversionLog = input.getString(0) + " |";

		String[] sentence = conversionLog.split("\\|");

		if (sentence.length < 19) {
			logger.error("conversionLog length loss error, lengthErrorlog : " + conversionLog);
			return;
		}

		String create_time = sentence[38];

		if (StringUtils.isEmpty(create_time) || create_time.length() != 19){
			logger.error("conversionLog create_time error , timeErrorLog : " + conversionLog);
			return;
		}

		try {
			timeDifferenceDF.parse(create_time);
		} catch (Exception e) {
			logger.error("timeDifferenceDF formt:" + conversionLog);
			return;
		}

		String campaign_id = sentence[2];
		String platform = sentence[4];
		String publisher_id = sentence[7];
		String publisher_slot = sentence[8];
		String publisher_type = sentence[9];
		String am_id = sentence[10];
		String advertiser_id = sentence[11];
		String advertiser_type = sentence[12];
		String bd_id = sentence[13];
		String pm_id = sentence[14];
		double revenue = Double.parseDouble(sentence[15]);
		double cost = Double.parseDouble(sentence[16]);
		double profit = revenue - cost;
		String day = DateUtil.getDay(sentence[38]);

		if (StringUtils.isEmpty(advertiser_id)
				|| StringUtils.isEmpty(advertiser_type)
				|| StringUtils.isEmpty(publisher_id)
				|| StringUtils.isEmpty(campaign_id)
				|| StringUtils.isEmpty(platform)
				|| StringUtils.isEmpty(publisher_type)
				|| StringUtils.isEmpty(am_id) || StringUtils.isEmpty(bd_id)
				|| StringUtils.isEmpty(pm_id)) {
			logger.error("conversion-log format error lack field: "
					+ conversionLog);
			return;
		}

		String mogoDBMapKey = PublicUtill.MD5(day + publisher_id
				+ publisher_slot + advertiser_id); // 拼接map中的key

		if (logNumber.get(day) == null) {

			for (Map.Entry<String, Integer> entry : logNumber.entrySet()) {
				logger.info("PSPMConversionToBaseReportBolt"
						+ entry.getKey() + "消费了" + entry.getValue() + "条数据！");
			}

			logNumber.clear();
			logNumber.put(day, 1);// 根据时间区分消费数据量

		} else {

			logNumber.put(day, logNumber.get(day) + 1);

		}

		int conversions = 0;
		float reportDayWeget = 0;
		float reportDayProfit = 0;

		conversions++;

		Document documentStr = new Document();
		Document documentInc = new Document();

		documentStr.append("_id", mogoDBMapKey).append("day", day)
				.append("publisher_slot", publisher_slot)
				.append("publisher_id", Integer.parseInt(publisher_id))
				.append("advertiser_id", Integer.parseInt(advertiser_id));

		documentInc.append("_id", mogoDBMapKey)
				.append("revenue", reportDayWeget + revenue)
				.append("profit", reportDayProfit + profit)
				.append("conversions", conversions);

		updateDocsInc.add(documentInc);
		updateDocsStr.add(documentStr);

		// 判断impressions的时间是否为一分钟之内或者mogoDBMap数据大于10000条进行mogodb存储
		if (updateDocsInc.size() > MOGONDB_NUM
				|| updateDocsStr.size() > MOGONDB_NUM) {

			insterOrUpdateDB();

		}
	}

	private void insterOrUpdateDB() {

		mongoUtil.bulkWriteUpdateInc(updateDocsInc);
		mongoUtil.bulkWriteUpdateStr(updateDocsStr);

		logger.info("PSPMConversionToBaseReportBolt 插入或更新"
				+ updateDocsInc.size() + "条数据！");

		updateDocsInc.clear();
		updateDocsStr.clear();
	}

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
