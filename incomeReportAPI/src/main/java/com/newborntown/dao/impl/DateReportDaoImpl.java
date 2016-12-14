package com.newborntown.dao.impl; 

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.client.AggregateIterable;
import com.newborntown.dao.DateReportDao;
import com.newborntown.utils.MongoUtil;

/** 
 * @author chenhao
 * @version 创建时间：2016年11月30日 上午10:28:19 
 */
public class DateReportDaoImpl implements DateReportDao {

	public double getUpstreamRevenue(MongoUtil mongoUtil, String date) {
		
		List<Document> pipeline = new ArrayList<Document>();
		
		Document match = new Document("$match",new Document("createdTime",date).append("network", "Facebook Audience Network"));
		pipeline.add(match);
		
		Document group = new Document();
		group.put("$group", new Document("_id", "$createdTime").append("total", new Document("$sum", "$show_revenue")));
		pipeline.add(group);
		
		AggregateIterable<Document> fb_income = mongoUtil.findCollection(pipeline);
		
		double fb_revenue = 0;
		
		if(null != fb_income.first()){
			String total = String.valueOf(fb_income.first().get("total"));
			
			total = total==null?"0":total;
	
			fb_revenue = Double.parseDouble(total)/10000;
		}
		
		return fb_revenue;
	}
	
}
