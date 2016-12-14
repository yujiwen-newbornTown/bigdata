package com.newborntown.dao.impl;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.newborntown.constant.Constant;
import com.newborntown.dao.PayOutDao;
import com.newborntown.utils.MongoUtil;

/**
 * @author  yujiwen E-mail: yujiwen@newborn-town.com
 * @version 创建时间：2016年11月30日 上午10:52:58
 * 
 */
public class PayOutDaoImpl implements PayOutDao{


	static String shuffle="shuffle",CMS="cm",facebook="fb",PingStart="PingStart",SoloRTB="SoloRTB",fb_audience="fb_audience",total="total";
	java.text.DecimalFormat   df   =new   java.text.DecimalFormat("0.0000");
	
	public static void main(String[] args) {
		startToPayOutMongoJob("2016:11:01");
	}
	
	
	public static void startToPayOutMongoJob(String date) {
		
		date = date.replaceAll("\\-", "\\:");
		
		MongoUtil income_mongoUtil = new MongoUtil(Constant.MONGODB_INCOME_DATABASE,Constant.MONGODB_INCOME_incomeReport
				,Constant.MOGODB_INCOME_HOST);
		
		MongoUtil dateReport_mongoUtil = new MongoUtil(Constant.MONGODB_DATAREPORT_DATABASE,Constant.MONGODB_DATAREPORT_dataReport
				,Constant.MOGODB_INCOME_HOST);
		
		MongoUtil payout_mongoUtil = new MongoUtil(Constant.MONGODB_INCOME_DATABASE,Constant.MONGODB_INCOME_payOut
				,Constant.MOGODB_INCOME_HOST);
		
		new PayOutDaoImpl().insertPayOut(income_mongoUtil,payout_mongoUtil,date);
		new SourceNumDaoImpl().insertFBAudienceSrcRevenue(dateReport_mongoUtil, payout_mongoUtil, date);
		
	}
	
	public void insertPayOut(MongoUtil mongoUtil,MongoUtil mongoUtilStatistic,String date){
		
		Document query = new Document();
		Document result = new Document();
		
		query.append("date",date);
		result.append("publisher_id", 1).append("shuffle_revenue", 2).append("CMS_revenue", 3).append("facebook_revenue", 4)
				.append("PingStart_revenue", 5).append("SoloRTB_revenue", 6).append("fb_audience_revenue", 7).append("slot_id", 8);
		
		FindIterable<Document> publisherList = mongoUtil.findDatas(query, result);
		
		HashMap<String,HashMap<String,Double>> outMap = new HashMap<String,HashMap<String,Double>>();
		
		MongoUtil mongoUtilSlot = new MongoUtil(Constant.MONGODB_SLOT_DATABASE, Constant.MOGODB_SLOT_slot, Constant.MOGODB_SLOT_HOST);
		SlotInfoDaoImpl slotInfoDaoImpl = new SlotInfoDaoImpl();
		HashMap<String, ArrayList<String>> pubSlotMap = slotInfoDaoImpl.getPublisherSlotMapping(mongoUtilSlot);
		
		for(Document doc : publisherList){
			String publisher_id = "",slot_id="";
			double shuffle_revenue = 0,CMS_revenue=0,facebook_revenue=0,PingStart_revenue=0,SoloRTB_revenue=0;
			HashMap<String,Double> srcRev = new HashMap<String,Double>();
			publisher_id = doc.getString("publisher_id");
			slot_id = doc.getString("slot_id");
			String key;
			if(publisher_id.contains(".")){
				key = publisher_id.split("\\.")[0]+"_"+date;
			}
			key = publisher_id+"_"+date;
			
			if(pubSlotMap.containsKey(publisher_id) && pubSlotMap.get(publisher_id).contains(slot_id)){
				if(doc.get("shuffle_revenue")!=null){
					shuffle_revenue = Double.parseDouble(doc.get("shuffle_revenue").toString());
				}
				if(doc.get("CMS_revenue")!=null){
					CMS_revenue = Double.parseDouble(doc.get("CMS_revenue").toString());
				}
				if(doc.get("facebook_revenue")!=null){
					facebook_revenue = Double.parseDouble(doc.get("facebook_revenue").toString());
				}
				if(doc.get("PingStart_revenue")!=null){
					PingStart_revenue = Double.parseDouble(doc.get("PingStart_revenue").toString());
				}
				if(doc.get("SoloRTB_revenue")!=null){
					SoloRTB_revenue = Double.parseDouble(doc.get("SoloRTB_revenue").toString());
				}
			}else{
				continue;
			}
					
			if(outMap.containsKey(key)){
				srcRev = outMap.get(key);
				srcRev.put(shuffle, srcRev.get(shuffle)+shuffle_revenue);
				srcRev.put(CMS, srcRev.get(CMS)+CMS_revenue);
				srcRev.put(facebook, srcRev.get(facebook)+facebook_revenue);
				srcRev.put(PingStart, srcRev.get(PingStart)+PingStart_revenue);
				srcRev.put(SoloRTB, srcRev.get(SoloRTB)+SoloRTB_revenue);
				
			}else{
				srcRev.put(shuffle, shuffle_revenue);
				srcRev.put(CMS, CMS_revenue);
				srcRev.put(facebook, facebook_revenue);
				srcRev.put(PingStart, PingStart_revenue);
				srcRev.put(SoloRTB, SoloRTB_revenue);
			}
			outMap.put(key, srcRev);
			
		}
		
		ArrayList<Document> documentStr = new ArrayList<Document>();
		ArrayList<Document> documentInc = new ArrayList<Document>();
		for(Map.Entry<String, HashMap<String,Double>> map :outMap.entrySet()){
			Document docSet = new Document();
			Document docInc = new Document();
			String[] pubDate = map.getKey().split("_");
			docSet.append("_id", map.getKey())
					.append("publisher_id", pubDate[0])
					.append("date", pubDate[1]);
			
			docInc.append("_id",map.getKey())
					.append(shuffle, Double.parseDouble(df.format(map.getValue().get(shuffle))))
					.append(CMS, Double.parseDouble(df.format(map.getValue().get(CMS))))
					.append(facebook, Double.parseDouble(df.format(map.getValue().get(facebook))))
					.append(PingStart, Double.parseDouble(df.format(map.getValue().get(PingStart))))
					.append(SoloRTB, Double.parseDouble(df.format(map.getValue().get(SoloRTB))));
			
			documentStr.add(docSet);
			documentInc.add(docInc);
		}
		
		mongoUtilStatistic.bulkWriteUpdateStr(documentStr);
		mongoUtilStatistic.bulkWriteUpdateInc(documentInc);
		
		documentStr.clear();
		documentInc.clear();
		
	}
	
	public LinkedHashMap<String,HashMap<String,String>> getPayOutOfPublisher(MongoUtil mongoUtilStatistic,String startDate,String endDate){
		
		LinkedHashMap<String,HashMap<String,Double>> resultMap= new LinkedHashMap<String,HashMap<String,Double>>();
		Document query = new Document();
		Document result = new Document();
		
		query.append("date", new BasicDBObject("$gte",startDate).append("$lte", endDate));
		result.append("publisher_id", 1).append(shuffle, 2).append(CMS, 3).append(facebook, 4)
				.append(PingStart, 5).append(SoloRTB, 6).append(fb_audience, 7).append(total, 8);
//				.append("impression", 8)
//				.append("show_click", 9)
//				.append("fill", 10)
//				.append("request", 11);
		
		FindIterable<Document> publisherList = mongoUtilStatistic.findDatas(query, result);
		
		for(Document doc : publisherList){
			
			String publisher_id="";
			double shuffle_revenue = 0,CMS_revenue=0,facebook_revenue=0,PingStart_revenue=0,SoloRTB_revenue=0,fb_audience_revenue=0;//,total_revenue=0;//,impression=0,show_click=0,fill=0,request=0;
			
			HashMap<String,Double> srcRec = new HashMap<String,Double>();
			publisher_id = doc.getString("publisher_id");
			
			shuffle_revenue = doc.getDouble(shuffle)==null?0:doc.getDouble(shuffle);
			CMS_revenue = doc.getDouble(CMS)==null?0:doc.getDouble(CMS);
			facebook_revenue = doc.getDouble(facebook)==null?0:doc.getDouble(facebook);
			PingStart_revenue = doc.getDouble(PingStart)==null?0:doc.getDouble(PingStart);
			SoloRTB_revenue = doc.getDouble(SoloRTB)==null?0:doc.getDouble(SoloRTB);
			fb_audience_revenue = doc.getDouble(fb_audience)==null?0:doc.getDouble(fb_audience);
//			total_revenue = doc.getDouble(total)==null?0:doc.getDouble(total);
			
//			if(doc.get("impression")!=null){
//				impression = Double.parseDouble(doc.get("impression").toString());
//			}
//			if(doc.get("show_click")!=null){
//				show_click = Double.parseDouble(doc.get("show_click").toString());
//			}
//			if(doc.get("fill")!=null){
//				fill = Double.parseDouble(doc.get("fill").toString());
//			}
//			if(doc.get("request")!=null){
//				request = Double.parseDouble(doc.get("request").toString());
//			}
			
			if(resultMap.containsKey(publisher_id)){
				srcRec = resultMap.get(publisher_id);
				
				srcRec.put(shuffle, srcRec.get(shuffle)+shuffle_revenue);
				srcRec.put(CMS, srcRec.get(CMS)+CMS_revenue);
				srcRec.put(facebook, srcRec.get(facebook)+facebook_revenue);
				srcRec.put(PingStart, srcRec.get(PingStart)+PingStart_revenue);
				srcRec.put(SoloRTB, srcRec.get(SoloRTB)+SoloRTB_revenue);
				srcRec.put(fb_audience, srcRec.get(fb_audience)+fb_audience_revenue);
//				srcRec.put(total, srcRec.get(total)+total_revenue);
				
//				srcRec.put("impression", srcRec.get("impression")+impression);
//				srcRec.put("show_click", srcRec.get("show_click")+show_click);
//				srcRec.put("fill", srcRec.get("fill")+fill);
//				srcRec.put("request", srcRec.get("request")+request);
				
			}else{
				srcRec.put(shuffle, shuffle_revenue);
				srcRec.put(CMS, CMS_revenue);
				srcRec.put(facebook, facebook_revenue);
				srcRec.put(PingStart, PingStart_revenue);
				srcRec.put(SoloRTB, SoloRTB_revenue);
				srcRec.put(fb_audience, fb_audience_revenue);
//				srcRec.put(total, total_revenue);
				
//				srcRec.put("request",request);
//				srcRec.put("fill",fill);
//				srcRec.put("impression",impression);
//				srcRec.put("show_click",show_click);
				
			}
			resultMap.put(publisher_id, srcRec);
		
		}
		
		LinkedHashMap <String, HashMap<String,String>> sortMap = new LinkedHashMap <String, HashMap<String,String>>();
		List<Map.Entry<String, HashMap<String,Double>>> infoIds =
			    new ArrayList<Map.Entry<String, HashMap<String,Double>>>(resultMap.entrySet());
		
		Collections.sort(infoIds, new Comparator<Map.Entry<String, HashMap<String,Double>>>() {   
		    public int compare(Map.Entry<String, HashMap<String,Double>> o1, Map.Entry<String, HashMap<String,Double>> o2) { 
		    	
		    	if(org.apache.commons.lang3.StringUtils.isEmpty(o1.getKey())){
		    		return -1;
		    	}
		    	if(org.apache.commons.lang3.StringUtils.isEmpty(o2.getKey())){
		    		return 1;
		    	}
		        return Double.parseDouble(o1.getKey().toString()) > Double.parseDouble(o2.getKey().toString())?1:-1;
		    }
		});
		
		
		
		for(Entry<String, HashMap<String, Double>> pub : infoIds){
			double total = 0;
			HashMap<String,String> inMap = new HashMap<String,String>();
			for( Map.Entry<String, Double> inm:pub.getValue().entrySet()){
				total += inm.getValue();
				inMap.put(inm.getKey(), df.format(inm.getValue()));
			}
			inMap.put("total", df.format(total));
			sortMap.put(pub.getKey(), inMap);
		}
		
//		System.out.println(sortMap.size()+" "+sortMap);
		
		return sortMap;
	}	
	/*public static void main(String[] args) {

		
//		MongoUtil income_mongoUtil = new MongoUtil(Constant.MONGODB_INCOME_DATABASE,Constant.MONGODB_INCOME_incomeReport
//				,Constant.MOGODB_INCOME_HOST);
//		
//		MongoUtil dateReport_mongoUtil = new MongoUtil(Constant.MONGODB_DATAREPORT_DATABASE,Constant.MONGODB_DATAREPORT_dataReport
//				,Constant.MOGODB_INCOME_HOST);
//		
//		MongoUtil payout_mongoUtil = new MongoUtil(Constant.MONGODB_INCOME_DATABASE,Constant.MONGODB_INCOME_payOut
//				,Constant.MOGODB_INCOME_HOST);
		
		//offline
		MongoUtil income_mongoUtil = new MongoUtil("java_pingstart","income_report",Constant.MOGODB_INCOME_HOST);
		MongoUtil dateReport_mongoUtil = new MongoUtil("new_pingstart","dateReport",Constant.MOGODB_INCOME_HOST);
		MongoUtil payout_mongoUtil = new MongoUtil("java_pingstart","Payout",Constant.MOGODB_INCOME_HOST);
		
		String date="2016:10:01";
		
		new PayOutDaoImpl().insertPayOut(income_mongoUtil,payout_mongoUtil,date);
		new SourceNumDaoImpl().insertFBAudienceSrcRevenue(dateReport_mongoUtil, payout_mongoUtil, date);
		new PayOutDaoImpl().getPayOutOfPublisher(payout_mongoUtil, "2016:10:01", "2016:11:03");
		
	}*/

	public HashMap<String, Double> getPayOutOfSource(MongoUtil mongoUtil, String[] sourceArray, String startDate,String endDate) {
		
		List<Document> pipeline = new ArrayList<Document>();
		
		HashMap<String,Double> map = new HashMap<String,Double>();
		
		Document group = new Document();
		
		Document match = new Document();
		
		Document query = new Document();
		
		query.append("_id", null);
		
		for (String sourceDoc : sourceArray) {
			
			query.append(sourceDoc, new Document("$sum","$"+sourceDoc));
		}
		
		match.put("$match", new Document("date", new BasicDBObject("$gte",startDate).append("$lte", endDate)));
		pipeline.add(match);
		
		group.put("$group", query);
		pipeline.add(group);
		
		AggregateIterable<Document> revenueDocs = mongoUtil.findCollection(pipeline);
		
		if(null !=revenueDocs){
			
			Document mongoDoc = revenueDocs.first();
			
			for (String source : sourceArray) {
				
				map.put(source, Double.parseDouble(String.valueOf(mongoDoc.get(source))==null?"0":String.valueOf(mongoDoc.get(source))));
			}
		}
		return map;
	}

}
