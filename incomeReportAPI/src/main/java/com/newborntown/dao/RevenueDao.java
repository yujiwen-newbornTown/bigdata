package com.newborntown.dao; 

import java.util.HashMap;

import com.newborntown.utils.MongoUtil;

/** 
 * @author chenhao
 * @version 创建时间：2016年11月30日 上午11:39:32 
 */
public interface RevenueDao {
	
	/**
	 * 通过mongo字段计算revenue
	 * @param mongoUtil
	 * @param source
	 * @return
	 */
	public HashMap<String,Double> getRevenue(MongoUtil mongoUtil,String[] source,String startDate,String endDate);

}
