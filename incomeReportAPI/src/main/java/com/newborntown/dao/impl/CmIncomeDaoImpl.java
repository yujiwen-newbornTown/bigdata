package com.newborntown.dao.impl; 

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.client.AggregateIterable;
import com.newborntown.dao.CmIncomeDao;
import com.newborntown.utils.MongoUtil;

/** 
 * @author chenhao
 * @version 创建时间：2016年11月30日 上午9:58:17 
 */
public class CmIncomeDaoImpl implements CmIncomeDao {

	public double getUpstreamRevenue(MongoUtil mongoUtil, String date) {
		
		List<Document> pipeline = new ArrayList<Document>();
		
		Document match = new Document("$match",new Document("date",date));
		pipeline.add(match);
		
		Document group = new Document();
		group.put("$group", new Document("_id", "$date").append("total", new Document("$sum", "$revenue")));
		pipeline.add(group);
		
		AggregateIterable<Document> cm_income = mongoUtil.findCollection(pipeline);
		
		double cm_revenue = 0;
		
		if(null != cm_income.first()){
			
			String total = String.valueOf(cm_income.first().get("total"));
			
			total = total==null?"0":total;
	
			cm_revenue = Double.parseDouble(total);
			
		}
		
		return cm_revenue;
	}

}
