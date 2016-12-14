package newborn_town.dao.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.bson.Document;

import newborn_town.commonpojo.ConversionReportMongoBean;
import newborn_town.commonpojo.ReportMongoBean;
import newborn_town.dao.BaseReportDao;
import newborn_town.dao.ConversionBaseReportDao;
import newborn_town.util.MongoUtil;

/**
 * @author  yujiwen E-mail: yujiwen@newborn-town.com
 * @version 创建时间：2016年11月24日 下午3:41:13
 * 
 */
public class ConversionBaseReportDaoImpl implements ConversionBaseReportDao{

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
			.append("publisher_id", Integer.parseInt(report.getPublisher_id()))
			.append("publisher_slot", report.getPublisher_slot())
			.append("advertiser_id", Integer.parseInt(report.getAdvertiser_id()));
			
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
