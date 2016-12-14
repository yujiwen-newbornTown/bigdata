package newborn_town.pspm.online.bolt;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import newborn_town.rawlogclean.RecordQueueManager;
import newborn_town.rawlogclean.TimeRecordBean;
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
 * 将campaign_id + publisher_id + click_ua + click_ip分组后的原始日志根据 MD5 进行分组
 * 
 * @author yangyang
 *
 */
public class PSPMClickToBaseReportBolt extends BaseBasicBolt {

	private static final long serialVersionUID = -6707182554696236571L;

	static Logger logger = Logger.getLogger(PSPMClickToBaseReportBolt.class);

	static int MOGONDB_NUM = 5000; // 批量存储Mogondb的阈值

	SimpleDateFormat timeDifferenceDF = null;

	int secondNum = 10;

	Queue<TimeRecordBean> recordQueue = null;

	Map<String, String> recordMap = null;

	MongoUtil mongoUtil = null;

	HashMap<String, Integer> logNumber = null;// log日志区分map

	ArrayList<Document> updateStrDocs = null;

	ArrayList<Document> updateIncDocs = null;

	public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
			org.apache.storm.task.TopologyContext context) {

		timeDifferenceDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		recordQueue = new LinkedList<TimeRecordBean>();
		recordMap = new HashMap<String, String>();
		mongoUtil = new MongoUtil("pspm", "baseReport");
		updateStrDocs = new ArrayList<Document>();
		updateIncDocs = new ArrayList<Document>();
		logNumber = new HashMap<String, Integer>();

	};

	@Override
	public void execute(Tuple input, BasicOutputCollector collector) {

		// 判断是否是定时任务的消息
		if (input.getSourceComponent().equals(Constants.SYSTEM_COMPONENT_ID)
				&& input.getSourceStreamId().equals(
						Constants.SYSTEM_TICK_STREAM_ID)) {

			if (updateIncDocs.size() > 0 && updateStrDocs.size() > 0) {

				insterOrUpdateDB();

			}

			return;
		}

		String[] sentence = (String[]) input.getValue(1);

		String str = (String) input.getValue(0);

		String create_time = sentence[1];
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
		String day = DateUtil.getDay(sentence[1]);

		if (StringUtils.isEmpty(advertiser_id)
				|| StringUtils.isEmpty(advertiser_type)
				|| StringUtils.isEmpty(publisher_id)
				|| StringUtils.isEmpty(campaign_id)
				|| StringUtils.isEmpty(platform)
				|| StringUtils.isEmpty(publisher_type)
				|| StringUtils.isEmpty(am_id) || StringUtils.isEmpty(bd_id)
				|| StringUtils.isEmpty(pm_id)) {
			logger.error("click-log format error lack field: " + sentence);
			return;
		}

		if (logNumber.get(day) == null) {

			for (Map.Entry<String, Integer> entry : logNumber.entrySet()) {
				logger.info("PSPMClickToBaseReportBolt" + entry.getKey()
						+ "消费了" + entry.getValue() + "条数据！");
			}

			logNumber.clear();
			logNumber.put(day, 1);// 根据时间区分消费数据量

		} else {

			logNumber.put(day, logNumber.get(day) + 1);

		}

		String mogoDBMapKey = PublicUtill.MD5(day + publisher_id
				+ publisher_slot + advertiser_id); // 拼接map中的key

		// 按照campaign_id+publisher_id+click_ua+click_ip过滤
		TimeRecordBean timeRecordBean = new TimeRecordBean(str, create_time);

		int unique_clicks = 0;
		int gross_clicks = 0;

		try {

			if (RecordQueueManager.updateRecordsQueue(timeRecordBean,
					recordQueue, recordMap, timeDifferenceDF)) {

				unique_clicks++;
			}

			gross_clicks++;

			Document documentStr = new Document();// 用来保存和更新不需要自增函数的字段
			Document documentInc = new Document();// 用来保存和更新需要累加的字段

			documentInc.append("_id", mogoDBMapKey)
					.append("gross_clicks", gross_clicks)
					.append("unique_clicks", unique_clicks);

			documentStr.append("_id", mogoDBMapKey).append("day", day)
					.append("publisher_id", Integer.parseInt(publisher_id))
					.append("publisher_slot", publisher_slot)
					.append("advertiser_id", Integer.parseInt(advertiser_id));

			updateIncDocs.add(documentInc);
			updateStrDocs.add(documentStr);

			if (updateIncDocs.size() > MOGONDB_NUM
					|| updateStrDocs.size() > MOGONDB_NUM) {

				insterOrUpdateDB();

			}

		} catch (Exception e) {

			logger.error(e);

		}
	}

	private void insterOrUpdateDB() {

		mongoUtil.bulkWriteUpdateInc(updateIncDocs);

		mongoUtil.bulkWriteUpdateStr(updateStrDocs);

		logger.info("PSPMClickToBaseReportBolt 插入或更新" + updateIncDocs.size()
				+ "条数据！");

		updateIncDocs.clear();
		updateStrDocs.clear();
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
