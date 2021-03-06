package com.newborntown.dao.impl; 

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.client.AggregateIterable;
import com.newborntown.dao.FbIncomeDao;
import com.newborntown.utils.MongoUtil;

/** 
 * @author chenhao
 * @version 创建时间：2016年11月30日 上午10:01:24 
 */
public class FbIncomeDaoImpl implements FbIncomeDao{

	public double getUpstreamRevenue(MongoUtil mongoUtil, String date) {
		
		List<Document> pipeline = new ArrayList<Document>();
		
		Document match = new Document("$match",new Document("date",date));
		pipeline.add(match);
		
		Document group = new Document();
		group.put("$group", new Document("_id", "$date").append("total", new Document("$sum", "$revenue")));
		pipeline.add(group);
		
		AggregateIterable<Document> fb_income = mongoUtil.findCollection(pipeline);
		
		double fb_revenue = 0;
		
		if(null != fb_income.first()){
			
			String total = String.valueOf(fb_income.first().get("total"));
			
			total = total==null?"0":total;
	
			fb_revenue = Double.parseDouble(total);
		}
		
		return fb_revenue;
	}

}
