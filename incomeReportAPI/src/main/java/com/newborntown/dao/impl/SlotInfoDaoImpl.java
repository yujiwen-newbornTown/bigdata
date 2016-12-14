package com.newborntown.dao.impl;

import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.bson.Document;

import com.mongodb.client.FindIterable;
import com.newborntown.constant.Constant;
import com.newborntown.dao.SlotInfoDao;
import com.newborntown.utils.MongoUtil;


public class SlotInfoDaoImpl implements SlotInfoDao{
	static Logger logger = Logger.getLogger(SlotInfoDaoImpl.class);
	
	@Override
	public HashMap<String, ArrayList<String>> getPublisherSlotMapping(MongoUtil mongoUtil) {
	
		
		HashMap<String, ArrayList<String>> publisherSlotMap = new HashMap<String, ArrayList<String>>();
		Document result = new Document();
		result.append("appId",1);

		
		FindIterable<Document> publisherSlot = mongoUtil.findDatas(null, result);
		String publisher_id="",slot_id="";
		
		for(Document doc : publisherSlot){
			
			ArrayList<String> slotList = new ArrayList<String>();
			slot_id = doc.getInteger("_id").toString();
			publisher_id = doc.get("appId").toString();

			if(publisherSlotMap.containsKey(publisher_id)){
				slotList = publisherSlotMap.get(publisher_id);
				if(!slotList.contains(slot_id)){
					slotList.add(slot_id);
				}
			}else{
				slotList.add(slot_id);
			}
			
			publisherSlotMap.put(publisher_id, slotList);
		}
		
		return publisherSlotMap;
	}

	public static void main(String[] args){
		MongoUtil mongoUtil = new MongoUtil(Constant.MONGODB_SLOT_DATABASE, Constant.MOGODB_SLOT_slot, Constant.MOGODB_SLOT_HOST);
//		MongoUtil mongoUtil = new MongoUtil(Constant.MONGODB_INCOME_DATABASE, Constant.MONGODB_INCOME_incomeReport, Constant.MOGODB_SLOT_HOST);
		SlotInfoDaoImpl impl = new SlotInfoDaoImpl();
		HashMap<String, ArrayList<String>> selfSlotMapping  = impl.getPublisherSlotMapping(mongoUtil);

//		for(Entry<String, ArrayList<String>> entry : selfSlotMapping.entrySet()){
//			System.out.println(entry.getKey() + "," + entry.getValue());
//
//		}
	}

}
