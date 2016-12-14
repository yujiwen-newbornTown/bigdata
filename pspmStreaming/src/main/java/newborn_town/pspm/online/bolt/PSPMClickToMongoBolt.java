package newborn_town.pspm.online.bolt;

import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

import newborn_town.commonpojo.ReportMongoBean;
import newborn_town.dao.ApiReportDao;
import newborn_town.dao.BaseReportDao;
import newborn_town.dao.CampaignReportDao;
import newborn_town.dao.CountryReportDao;
import newborn_town.dao.impl.ApiReportDaoImpl;
import newborn_town.dao.impl.BaseReportDaoImpl;
import newborn_town.dao.impl.CampaignReportDaoImpl;
import newborn_town.dao.impl.CountryReportDaoImpl;
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
 * @author  yujiwen E-mail: yujiwen@newborn-town.com
 * @version 创建时间：2016年12月12日 下午5:51:55
 * 
 */
public class PSPMClickToMongoBolt extends BaseBasicBolt {


	private static final long serialVersionUID = 3694898337662243758L;

	static Logger logger = Logger.getLogger(PSPMClickToMongoBolt.class);

	static int MOGONDB_NUM = 5000; // 批量存储Mogondb的阈值

	SimpleDateFormat timeDifferenceDF = null;
	int secondNum = 10;

	Queue<TimeRecordBean> recordQueue = null;
	Map<String, String> recordMap = null;

	HashMap<String, Integer> logNumber = null;// log日志区分map
	ArrayList<Document> updateStrDocs = null;
	ArrayList<Document> updateIncDocs = null;
	
	MongoUtil mongoUtilApi = null;
	MongoUtil mongoUtilBase = null;
	MongoUtil mongoUtilCampaign = null;
	MongoUtil mongoUtilCountry = null;
	
	IdentityHashMap<String,ReportMongoBean> apiUpdateMap = null;
	IdentityHashMap<String,ReportMongoBean> baseUpdateMap = null;
	IdentityHashMap<String,ReportMongoBean> campaignUpdateMap = null;
	IdentityHashMap<String,ReportMongoBean> countryUpdateMap = null;
	
	ApiReportDao apiReportDao = null;
	BaseReportDao baseReportDao = null;
	CampaignReportDao campaignReportDao = null;
	CountryReportDao countryReportDao = null;

	public void prepare(@SuppressWarnings("rawtypes") Map stormConf,
			org.apache.storm.task.TopologyContext context) {

		timeDifferenceDF = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		recordQueue = new LinkedList<TimeRecordBean>();
		recordMap = new HashMap<String, String>();

		mongoUtilApi = new MongoUtil("pspm", "apiReport");
		mongoUtilBase = new MongoUtil("pspm", "baseReport");
		mongoUtilCampaign = new MongoUtil("pspm", "campaignReport");
		mongoUtilCountry = new MongoUtil("pspm", "countryReport");
		
		apiUpdateMap = new IdentityHashMap<String,ReportMongoBean>();
		baseUpdateMap = new IdentityHashMap<String,ReportMongoBean>();
		campaignUpdateMap = new IdentityHashMap<String,ReportMongoBean>();
		countryUpdateMap = new IdentityHashMap<String,ReportMongoBean>();
		
		apiReportDao = new ApiReportDaoImpl();
		baseReportDao = new BaseReportDaoImpl();
		campaignReportDao = new CampaignReportDaoImpl();
		countryReportDao = new CountryReportDaoImpl();
		
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

			if (apiUpdateMap.size() > 0 || baseUpdateMap.size() > 0 || campaignUpdateMap.size() > 0 || countryUpdateMap.size() > 0) {
				
				apiReportDao.insertIntoMongo(apiUpdateMap, mongoUtilApi);
				apiUpdateMap.clear();
				baseReportDao.insertIntoMongo(baseUpdateMap, mongoUtilBase);
				baseUpdateMap.clear();
				campaignReportDao.insertIntoMongo(campaignUpdateMap, mongoUtilCampaign);
				campaignUpdateMap.clear();
				countryReportDao.insertIntoMongo(countryUpdateMap, mongoUtilCountry);
				countryUpdateMap.clear();
				
			}

			return;
		}

		String[] sentence = (String[]) input.getValue(1);

		String str = (String) input.getValue(0);

		String create_time = sentence[38];
		String campaign_id = sentence[2];
		String platform = sentence[4];
		String geo = sentence[5];
		String publisher_id = sentence[7];
		String publisher_slot = sentence[8];
		String publisher_type = sentence[9];
		String am_id = sentence[10];
		String advertiser_id = sentence[11];
		String advertiser_type = sentence[12];
		String bd_id = sentence[13];
		String pm_id = sentence[14];
		String day = DateUtil.getDay(sentence[38]);

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
				logger.info("PSPMClickToMongoBolt" + entry.getKey()
						+ "消费了" + entry.getValue() + "条数据！");
			}

			logNumber.clear();
			logNumber.put(day, 1);// 根据时间区分消费数据量

		} else {

			logNumber.put(day, logNumber.get(day) + 1);

		}

		
		String mogoDBApiMapKey = PublicUtill.MD5(day + publisher_slot + geo
				+ campaign_id + publisher_id); // 拼接map中的key
		String mogoDBBaseMapKey = PublicUtill.MD5(day + publisher_id
				+ publisher_slot + advertiser_id); // 拼接map中的key
		String mogoDBCampaignMapKey = PublicUtill.MD5(day + campaign_id); // 拼接map中的key
		String mogoDBCountryMapKey = PublicUtill.MD5(day + geo + advertiser_id
				+ publisher_id); // 拼接map中的key

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

			ReportMongoBean reportMongoBean = new ReportMongoBean();
			
			reportMongoBean.setDay(day);
			reportMongoBean.setCampaign_id(campaign_id);
			reportMongoBean.setCountry(geo);
			reportMongoBean.setGross_clicks(gross_clicks);		
			reportMongoBean.setAdvertiser_id(advertiser_id);
			reportMongoBean.setPublisher_slot(publisher_slot);
			reportMongoBean.setPublisher_id(publisher_id);
			reportMongoBean.setUnique_clicks(unique_clicks);
			
			apiUpdateMap.put(mogoDBApiMapKey, reportMongoBean);
			baseUpdateMap.put(mogoDBBaseMapKey, reportMongoBean);
			campaignUpdateMap.put(mogoDBCampaignMapKey, reportMongoBean);
			countryUpdateMap.put(mogoDBCountryMapKey, reportMongoBean);

			if (apiUpdateMap.size() > MOGONDB_NUM || baseUpdateMap.size() > MOGONDB_NUM || campaignUpdateMap.size() > MOGONDB_NUM || countryUpdateMap.size() > MOGONDB_NUM) {

				apiReportDao.insertIntoMongo(apiUpdateMap, mongoUtilApi);
				apiUpdateMap.clear();
				baseReportDao.insertIntoMongo(baseUpdateMap, mongoUtilBase);
				baseUpdateMap.clear();
				campaignReportDao.insertIntoMongo(campaignUpdateMap, mongoUtilCampaign);
				campaignUpdateMap.clear();
				countryReportDao.insertIntoMongo(countryUpdateMap, mongoUtilCountry);
				countryUpdateMap.clear();

			}

		} catch (Exception e) {

			logger.error(e);

		}
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
