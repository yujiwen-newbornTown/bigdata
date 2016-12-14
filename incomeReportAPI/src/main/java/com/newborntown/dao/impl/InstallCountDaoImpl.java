package com.newborntown.dao.impl; 

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.bson.Document;

import com.mongodb.client.AggregateIterable;
import com.newborntown.dao.InstallCountDao;
import com.newborntown.utils.MongoUtil;

/** 
 * @author chenhao
 * @version 创建时间：2016年11月30日 上午10:05:54 
 */
public class InstallCountDaoImpl implements InstallCountDao {

	public HashMap<String, Double> getUpstreamRevenue(MongoUtil mongoUtil,
			String date) {
		
		List<Document> pipeline = new ArrayList<Document>();
		
		Document match = new Document("$match",new Document("date",date));
		pipeline.add(match);
		
		Document group = new Document();
		group.put("$group", new Document("_id", "$source").append("total", new Document("$sum", "$revenue")));
		pipeline.add(group);
		
		AggregateIterable<Document> install_income = mongoUtil.findCollection(pipeline);
		
		HashMap<String, Double> installSourceMap = new HashMap<String, Double>();
		
		for (Document doc : install_income) {
			
			installSourceMap.put(doc.getString("_id"),Double.parseDouble(String.valueOf(doc.get("total"))==null?"0":String.valueOf(doc.get("total"))));
		}
		
		return installSourceMap;
	}

}
