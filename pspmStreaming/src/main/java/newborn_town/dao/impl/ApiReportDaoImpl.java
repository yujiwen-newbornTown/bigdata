package newborn_town.dao.impl;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Map.Entry;

import newborn_town.commonpojo.ReportMongoBean;
import newborn_town.dao.ApiReportDao;
import newborn_town.util.MongoUtil;

import org.bson.Document;

/**
 * @author  yujiwen E-mail: yujiwen@newborn-town.com
 * @version 创建时间：2016年11月24日 上午11:39:35
 * 
 */
public class ApiReportDaoImpl implements ApiReportDao {

	
	@Override
	public ArrayList<Document> getApiReportDocument(MongoUtil mongoUtil) {
		
		return null;
	}

	@Override
	public void insertIntoMongo(IdentityHashMap<String, ReportMongoBean> reportMap,
			MongoUtil mongoUtil) {
		
		ArrayList<Document> docStrList = new ArrayList<Document>();
		ArrayList<Document> docIncList = new ArrayList<Document>();
		
		for(Entry<String, ReportMongoBean> reportMongoBeans: reportMap.entrySet()){
			
			Document docStr = new Document();
			Document docInc = new Document();
			
			ReportMongoBean report = reportMongoBeans.getValue();
			
			
			String key = reportMongoBeans.getKey();
			
			docStr.append("_id", key).append("day", report.getDay())
			.append("publisher_slot", report.getPublisher_slot())
			.append("country", report.getCountry())
			.append("campaign_id", Integer.parseInt(report.getCampaign_id()))
			.append("publisher_id", Integer.parseInt(report.getPublisher_id()));
			
			docInc
			.append("_id",key)
			.append("gross_clicks", report.getGross_clicks())
			.append("unique_clicks", report.getUnique_clicks());
			
			docStrList.add(docStr);
			docIncList.add(docInc);
			
	}
		
		mongoUtil.bulkWriteUpdateStr(docStrList);
		mongoUtil.bulkWriteUpdateInc(docIncList);
		docStrList.clear();
		docIncList.clear();
		
	}
	
	public static void main(String[] args) {

	}

}
