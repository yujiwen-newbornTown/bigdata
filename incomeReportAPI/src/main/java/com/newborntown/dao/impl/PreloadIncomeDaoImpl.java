package com.newborntown.dao.impl; 

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.client.AggregateIterable;
import com.newborntown.dao.PreloadIncomeDao;
import com.newborntown.utils.MongoUtil;

/** 
 * @author chenhao
 * @version 创建时间：2016年11月30日 上午10:03:55 
 */
public class PreloadIncomeDaoImpl implements PreloadIncomeDao {

	public double getUpstreamRevenue(MongoUtil mongoUtil, String date) {
		
		List<Document> pipeline = new ArrayList<Document>();
		
		Document match = new Document("$match",new Document("date",date));
		pipeline.add(match);
		
		Document group = new Document();
		group.put("$group", new Document("_id", "$date").append("total", new Document("$sum", "$revenue")));
		pipeline.add(group);
		
		AggregateIterable<Document> preload_income = mongoUtil.findCollection(pipeline);
		
		double preload_revenue = 0;
		
		if(null != preload_income.first()){
			
			String total = String.valueOf(preload_income.first().get("total"));
			
			total = total==null?"0":total;
	
			preload_revenue = Double.parseDouble(total);
		}
		
		return preload_revenue;
	}

}
