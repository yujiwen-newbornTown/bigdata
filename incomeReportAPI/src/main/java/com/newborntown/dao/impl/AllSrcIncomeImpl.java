package com.newborntown.dao.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.bson.Document;

import com.mongodb.client.FindIterable;
import com.newborntown.constant.Constant;
import com.newborntown.dao.AllSrcIncome;
import com.newborntown.utils.MongoUtil;

/**
 * @author  yujiwen E-mail: yujiwen@newborn-town.com
 * @version 创建时间：2016年12月5日 下午3:37:04
 * 
 */
public class AllSrcIncomeImpl implements AllSrcIncome {

	java.text.DecimalFormat   df   =new   java.text.DecimalFormat("0.0000");
	
	public static void main(String[] args){
		MongoUtil payout_mongoUtil = new MongoUtil("java_pingstart","Payout",Constant.MOGODB_INCOME_HOST);
		new AllSrcIncomeImpl().insertPayOut(payout_mongoUtil,"2016:11:29");
	}
	
	@Override
	public void insertPayOut(MongoUtil mongoUtilStatistic,String date) {
		Document query = new Document();
		Document result = new Document();
		
		query.append("date",date);
		result.append("publisher_id", 1).append("shuffle", 2).append("cm", 3).append("fb", 4)
				.append("PingStart", 5).append("SoloRTB", 6).append("fb_audience", 7);
		
		FindIterable<Document> publisherList = mongoUtilStatistic.findDatas(query, result);
		
		HashMap<String,Double> allIncome = new HashMap<String,Double>();
		
		for(Document doc : publisherList){
			
			String publisher_id = "";
			double shuffle_revenue = 0,CMS_revenue=0,facebook_revenue=0,PingStart_revenue=0,SoloRTB_revenue=0,fb_audience_revenue=0;
			
			publisher_id = doc.getString("publisher_id");
			String key;
			if(publisher_id.contains(".")){
				key = publisher_id.split("\\.")[0]+"_"+date;
			}
			key = publisher_id+"_"+date;
			
			if(doc.get("shuffle")!=null){
				shuffle_revenue = Double.parseDouble(doc.get("shuffle").toString());
			}
			if(doc.get("cm")!=null){
				CMS_revenue = Double.parseDouble(doc.get("cm").toString());
			}
			if(doc.get("facebook_revenue")!=null){
				facebook_revenue = Double.parseDouble(doc.get("fb").toString());
			}
			if(doc.get("PingStart")!=null){
				PingStart_revenue = Double.parseDouble(doc.get("PingStart").toString());
			}
			if(doc.get("SoloRTB")!=null){
				SoloRTB_revenue = Double.parseDouble(doc.get("SoloRTB").toString());
			}
			if(doc.get("fb_audience")!=null){
				fb_audience_revenue = Double.parseDouble(doc.get("fb_audience").toString());
			}
			
			allIncome.put(key, shuffle_revenue + CMS_revenue + facebook_revenue + PingStart_revenue + SoloRTB_revenue + fb_audience_revenue);
		}
		
		ArrayList<Document> documentInc = new ArrayList<Document>();
		for(Map.Entry<String, Double> map :allIncome.entrySet()){
			Document docInc = new Document();
			docInc.append("_id",map.getKey())
				.append("total",Double.parseDouble(df.format(map.getValue())));
			
			documentInc.add(docInc);
		}
		
		mongoUtilStatistic.bulkWriteUpdateInc(documentInc);
		documentInc.clear();
		
	}

	
}
