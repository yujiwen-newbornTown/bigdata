package com.newborntown.dao;

import java.util.HashMap;

import com.newborntown.utils.MongoUtil;

/**
 * @author  yujiwen E-mail: yujiwen@newborn-town.com
 * @version 创建时间：2016年11月30日 下午5:00:15
 * 
 */
public interface SourceNumDao {
	/**
	 * 统计dateReport表
	 * @param mongoUtilDateReport  dateReport mongo client
	 * @param mongoUtilStatistic   Payout  mongo client
	 * @param date  计算日期
	 */
	public void insertFBAudienceSrcRevenue(MongoUtil mongoUtilDateReport,MongoUtil mongoUtilStatistic,String date);
	/**
	 * 
	 * @param mongoUtilStatistic
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public HashMap<String,HashMap<String,Double>> getSrcRevenue(MongoUtil mongoUtilStatistic,String startDate, String endDate);
}
