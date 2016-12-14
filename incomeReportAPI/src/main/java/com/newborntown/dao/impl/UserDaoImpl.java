package com.newborntown.dao.impl; 

import java.util.HashMap;

import org.bson.Document;

import com.mongodb.client.FindIterable;
import com.newborntown.dao.UserDao;
import com.newborntown.utils.MongoUtil;

/** 
 * @author chenhao
 * @version 创建时间：2016年12月2日 上午11:30:50 
 */
public class UserDaoImpl implements UserDao {

	public HashMap<String, String> getPublisherIdAndEmail(MongoUtil mongoUtil) {
		
		FindIterable<Document> userPublisherEmailDoc = mongoUtil.findDatas(null, new Document().append("username", 1));
		
		HashMap<String, String> userPublisherEmailMap = new HashMap<String, String>();
		
		if(null != userPublisherEmailDoc){
			
			for (Document doc : userPublisherEmailDoc) {
				userPublisherEmailMap.put(String.valueOf(doc.get("_id")).replaceAll("\\.0", ""), doc.getString("username"));
			}
		}
		return userPublisherEmailMap;
	}
}
