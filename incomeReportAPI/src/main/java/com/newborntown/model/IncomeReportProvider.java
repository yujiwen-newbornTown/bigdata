package com.newborntown.model;

import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map.Entry;

import org.apache.commons.lang3.StringUtils;
import org.apache.log4j.Logger;

import com.alibaba.fastjson.JSON;
import com.newborntown.constant.Constant;
import com.newborntown.dao.PayOutDao;
import com.newborntown.dao.RevenueDao;
import com.newborntown.dao.UserDao;
import com.newborntown.dao.impl.PayOutDaoImpl;
import com.newborntown.dao.impl.RevenueDaoImpl;
import com.newborntown.dao.impl.UserDaoImpl;
import com.newborntown.utils.MongoUtil;



public class IncomeReportProvider {
	static Logger logger = Logger.getLogger(IncomeReportProvider.class);
	
	String[] sourceArray = {"SoloRTB","PingStart","cm","fb","shuffle","fb_audience"};
	
	PayOutDao payDao = new PayOutDaoImpl();
	RevenueDao rDao = new RevenueDaoImpl();
	UserDao userDao = new UserDaoImpl();
	
	MongoUtil revenue_mongo = new MongoUtil(Constant.MONGODB_INCOME_DATABASE, Constant.MONGODB_INCOME_revenue,Constant.MOGODB_INCOME_HOST);
	MongoUtil payOut_mongoUtil = new MongoUtil(Constant.MONGODB_INCOME_DATABASE,Constant.MONGODB_INCOME_payOut,Constant.MOGODB_INCOME_HOST);
	MongoUtil user_mongoUtil = new MongoUtil(Constant.MONGODB_INCOME_DATABASE,Constant.MONGODB_INCOME_user,Constant.MOGODB_INCOME_HOST);
	
	public static void main(String[] args) {
		System.out.println(new IncomeReportProvider().getRevenuePayOutProfitOfSource("2016:12:01", "2016:12:01"));
	}
	
	/**
	 * 获取各个source 的 payout
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public String getPayOutOfPublisher(String startDate,String endDate){

		HashMap<String,String> publisheridEmail_Map = userDao.getPublisherIdAndEmail(user_mongoUtil); 
		
		LinkedHashMap<String,HashMap<String,String>> sourceImComeMap = payDao.getPayOutOfPublisher(payOut_mongoUtil, startDate, endDate);
		
		LinkedHashMap<String,HashMap<String,String>> sourcePublisheridEmailMap = new LinkedHashMap<String, HashMap<String,String>>();
		
		for (Entry<String, HashMap<String, String>> sourceMap : sourceImComeMap.entrySet()) {
			
			String publisherId = sourceMap.getKey();
			
			String userEmail = publisheridEmail_Map.get(publisherId);
			
			if(StringUtils.isNotEmpty(userEmail)&& null!=userEmail){
				
				publisherId = publisherId + "|" + userEmail;
				
				sourcePublisheridEmailMap.put(publisherId, sourceMap.getValue());
			}
		}
		
		return JSON.toJSONString(sourcePublisheridEmailMap);
		
	}
	
	/**
	 * 获取各个source 的 revenue payout profit
	 * @param startDate
	 * @param endDate
	 * @return
	 */
	public String getRevenuePayOutProfitOfSource(String startDate,String endDate){
		
		HashMap<String,Double> revenueMap = rDao.getRevenue(revenue_mongo, sourceArray,startDate,endDate);
		
		HashMap<String,Double> payOutMap = payDao.getPayOutOfSource(payOut_mongoUtil,sourceArray, startDate, endDate);
		
		HashMap<String,HashMap<String,String>> revenuePayOutProfit_Map = new HashMap<String,HashMap<String,String>>();
		
		for (String source : sourceArray) {
			
			LinkedHashMap<String,String> map = new LinkedHashMap<String, String>();
			
			DecimalFormat formater = new DecimalFormat("#0.0000");
		    
			double revenue = revenueMap.get(source);
			double payOut = payOutMap.get(source);
			double profit = revenue-payOut;
			
			map.put("revenue", formater.format(revenue));
			map.put("payOut", formater.format(payOut));
			map.put("profit", formater.format(profit));
			
			revenuePayOutProfit_Map.put(source, map);
			
		}
		
		return JSON.toJSONString(revenuePayOutProfit_Map);
		
	}

}
