package com.newborntown.dao.impl; 

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.client.AggregateIterable;
import com.newborntown.dao.RevenueDao;
import com.newborntown.utils.MongoUtil;

/** 
 * @author chenhao
 * @version 创建时间：2016年11月30日 上午11:40:26 
 */
public class RevenueDaoImpl implements RevenueDao {
	
	/**
	 * 通过mongo字段计算revenue
	 */
	public HashMap<String,Double> getRevenue(MongoUtil mongoUtil,String[] sourceArray,String startDate,String endDate) {
		
		List<Document> pipeline = new ArrayList<Document>();
		
		HashMap<String,Double> map = new HashMap<String,Double>();
		
		Document group = new Document();
		
		Document match = new Document("$match", new Document("_id", new BasicDBObject("$gte",startDate).append("$lte", endDate)));
		
		Document query = new Document();
		
		query.append("_id", null);
		
		for (String sourceDoc : sourceArray) {
			
			query.append(sourceDoc, new Document("$sum","$"+sourceDoc));
		}
		pipeline.add(match);
		
		group.put("$group", query);
		pipeline.add(group);
		
		AggregateIterable<Document> revenueDocs = mongoUtil.findCollection(pipeline);
		
		if(null !=revenueDocs.first()){
			
			Document mongoDoc = revenueDocs.first();
			
			for (String source : sourceArray) {
				
				map.put(source, Double.parseDouble(String.valueOf(mongoDoc.get(source))==null?"0":String.valueOf(mongoDoc.get(source))));
			}
		}
		
		return map;
	}

}
