package com.newborntown.dao.impl;

import java.util.ArrayList;
import java.util.HashMap;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.client.FindIterable;
import com.newborntown.constant.Constant;
import com.newborntown.dao.SourceNumDao;
import com.newborntown.utils.MongoUtil;

/**
 * @author  yujiwen E-mail: yujiwen@newborn-town.com
 * @version 创建时间：2016年11月30日 下午5:01:34
 * 
 */
public class SourceNumDaoImpl implements SourceNumDao {

	java.text.DecimalFormat   df   =new   java.text.DecimalFormat("0.0000");
	
	public void insertFBAudienceSrcRevenue(MongoUtil dateReport_mongoUtil,
			MongoUtil payout_mongoUtil, String date) {

		Document query = new Document();
		Document result = new Document();
		
		query.append("createdTime",date.replace(":", "-"));
		result.append("publisher_id", 1).append("show_revenue", 2).append("network", 3)
				.append("impression", 4).append("show_click", 5).append("fill", 6).append("request", 7).append("slot_id", 8);
		
		FindIterable<Document> publisherList = dateReport_mongoUtil.findDatas(query, result);
		
		ArrayList<Document> documentStr = new ArrayList<Document>();
		ArrayList<Document> documentInc = new ArrayList<Document>();
		
		MongoUtil mongoUtilSlot = new MongoUtil(Constant.MONGODB_SLOT_DATABASE, Constant.MOGODB_SLOT_slot, Constant.MOGODB_SLOT_HOST);
		SlotInfoDaoImpl slotInfoDaoImpl = new SlotInfoDaoImpl();
		HashMap<String, ArrayList<String>> pubSlotMap = slotInfoDaoImpl.getPublisherSlotMapping(mongoUtilSlot);
		
		for(Document doc : publisherList){
			String key = "", publisher_id = "",slot_id="";
			double fb_audience_revenue = 0;
			double impression_num = 0, click_num = 0, fill_num = 0, request_num = 0 ;
			Document docStr = new Document();
			Document docInc = new Document();
			
			publisher_id = doc.getString("publisher_id");
			slot_id = doc.getInteger("slot_id").toString();
			
			if(publisher_id.contains(".")){
				key = publisher_id.split("\\.")[0]+"_"+date;
			}
			key = publisher_id+"_"+date;
			
			if(doc.getString("network").equalsIgnoreCase("Facebook Audience Network")){
				if(pubSlotMap.containsKey(publisher_id) && pubSlotMap.get(publisher_id).contains(slot_id)){
					fb_audience_revenue =  Double.parseDouble(doc.get("show_revenue").toString())/10000;
				}
				
			}else{

				if(doc.get("impression")!=null){
					impression_num = Double.parseDouble(doc.get("impression").toString());
				}
				if(doc.get("show_click")!=null){
					click_num = Double.parseDouble(doc.get("show_click").toString());
				}
				if(doc.get("fill")!=null){
					fill_num = Double.parseDouble(doc.get("fill").toString());
				}
				if(doc.get("request")!=null){
					request_num = Double.parseDouble(doc.get("request").toString());
				}
				
			}
			
			
			docStr.append("_id", key).append("publisher_id", publisher_id).append("date", date);
			
			docInc.append("_id", key)
				.append("fb_audience", Double.parseDouble(df.format(fb_audience_revenue)))
				.append("impression", impression_num)
				.append("show_click", click_num)
				.append("fill", fill_num)
				.append("request", request_num);
		
			documentStr.add(docStr);
			documentInc.add(docInc);
		}
		
		payout_mongoUtil.bulkWriteUpdateStr(documentStr);
		payout_mongoUtil.bulkWriteUpdateInc(documentInc);
		
		documentStr.clear();
		documentInc.clear();
		
	
	}

	
	public HashMap<String, HashMap<String, Double>> getSrcRevenue(
			MongoUtil mongoUtilStatistic, String startDate, String endDate) {

		HashMap<String,HashMap<String,Double>> resultMap= new HashMap<String,HashMap<String,Double>>();
		Document query = new Document();
		Document result = new Document();
		
		query.append("date", new BasicDBObject("$gte",startDate).append("$lte", endDate));
		result.append("publisher_id", 1)
				.append("impression", 2)
				.append("show_click", 3)
				.append("fill", 4)
				.append("request", 5);
		
		FindIterable<Document> publisherList = mongoUtilStatistic.findDatas(query, result);
		
		for(Document doc : publisherList){
			
			double impression=0,show_click=0,fill=0,request=0;
			HashMap<String,Double> srcRec = new HashMap<String,Double>();
			String publisher_id = doc.getString("publisher_id");

			if(doc.get("impression")!=null){
				impression = Double.parseDouble(doc.get("impression").toString());
			}
			if(doc.get("show_click")!=null){
				show_click = Double.parseDouble(doc.get("show_click").toString());
			}
			if(doc.get("fill")!=null){
				fill = Double.parseDouble(doc.get("fill").toString());
			}
			if(doc.get("request")!=null){
				request = Double.parseDouble(doc.get("request").toString());
			}
						
			if(resultMap.containsKey(publisher_id)){
				srcRec = resultMap.get(publisher_id);
				srcRec.put("impression", srcRec.get("impression")+impression);
				srcRec.put("show_click", srcRec.get("show_click")+show_click);
				srcRec.put("fill", srcRec.get("fill")+fill);
				srcRec.put("request", srcRec.get("request")+request);
			}else{
				srcRec.put("impression",impression);
				srcRec.put("show_click",show_click);
				srcRec.put("fill",fill);
				srcRec.put("request",request);
			}
			resultMap.put(publisher_id, srcRec);
		}
		
		return resultMap;
	
	}

}
