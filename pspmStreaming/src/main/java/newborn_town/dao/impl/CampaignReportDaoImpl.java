package newborn_town.dao.impl;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.Map;
import java.util.Map.Entry;

import org.bson.Document;

import newborn_town.commonpojo.ReportMongoBean;
import newborn_town.dao.CampaignReportDao;
import newborn_town.util.MongoUtil;

/**
 * @author  yujiwen E-mail: yujiwen@newborn-town.com
 * @version 创建时间：2016年11月24日 下午3:48:13
 * 
 */
public class CampaignReportDaoImpl implements CampaignReportDao{

	public static void main(String[] args) {

	}

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
			.append("campaign_id", Integer.parseInt(report.getCampaign_id()));
			
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

}
