package newborn_town.dao.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map.Entry;

import newborn_town.commonpojo.ConversionReportMongoBean;
import newborn_town.dao.ConversionCampaignReportDao;
import newborn_town.util.MongoUtil;

import org.bson.Document;

/**
 * @author  yujiwen E-mail: yujiwen@newborn-town.com
 * @version 创建时间：2016年11月24日 下午3:48:13
 * 
 */
public class ConversionCampaignReportDaoImpl implements ConversionCampaignReportDao{

	public static void main(String[] args) {

	}

	@Override
	public ArrayList<Document> getApiReportDocument(MongoUtil mongoUtil) {
		return null;
	}

	@Override
	public void insertIntoMongo(
			IdentityHashMap<String, ConversionReportMongoBean> reportMap,
			MongoUtil mongoUtil) {

		
		ArrayList<Document> docStrList = new ArrayList<Document>();
		ArrayList<Document> docIncList = new ArrayList<Document>();
		
		for(Entry<String, ConversionReportMongoBean> reportMongoBeans: reportMap.entrySet()){
			
			Document docStr = new Document();
			Document docInc = new Document();
			
			ConversionReportMongoBean report = reportMongoBeans.getValue();
			
			
			String key = reportMongoBeans.getKey();
			
			docStr.append("_id", key).append("day", report.getDay())
			.append("campaign_id", Integer.parseInt(report.getCampaign_id()));
			
			docInc
			.append("_id",key)
			.append("revenue", report.getRevenue())
			.append("profit", report.getProfit())
			.append("conversions", report.getConversions());	
			
			docStrList.add(docStr);
			docIncList.add(docInc);
			
	}
		
		mongoUtil.bulkWriteUpdateStr(docStrList);
		mongoUtil.bulkWriteUpdateInc(docIncList);
		docStrList.clear();
		docIncList.clear();
		
	
	}



}
