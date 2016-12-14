package com.newborntown.dao;

import java.util.HashMap;
import java.util.LinkedHashMap;

import com.newborntown.utils.MongoUtil;

/**
 * @author  yujiwen E-mail: yujiwen@newborn-town.com
 * @version 创建时间：2016年11月30日 上午10:50:20
 * 
 */
public interface PayOutDao {
	
	public void insertPayOut(MongoUtil mongoUtil,MongoUtil mongoUtilStatistic,String date);
	
	/**
	 * <publisherid,<source,revenue>>
	 * @param mongoUtil 
	 * @param startDate 开始日期
	 * @param endDate  结束日期
	 * @return    按publisher——id排序后的map
	 */
	public LinkedHashMap<String,HashMap<String,String>> getPayOutOfPublisher(MongoUtil mongoUtil,String startDate, String endDate);

	public HashMap<String, Double> getPayOutOfSource(MongoUtil payOut_mongoUtil, String[] sourceArray, String startDate,
			String endDate);
}
