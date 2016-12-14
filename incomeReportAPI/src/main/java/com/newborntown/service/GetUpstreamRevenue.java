package com.newborntown.service; 


import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Map.Entry;

import org.bson.Document;

import com.newborntown.constant.Constant;
import com.newborntown.dao.CmIncomeDao;
import com.newborntown.dao.DateReportDao;
import com.newborntown.dao.FbIncomeDao;
import com.newborntown.dao.InstallCountDao;
import com.newborntown.dao.PreloadIncomeDao;
import com.newborntown.dao.RevenueDao;
import com.newborntown.dao.impl.CmIncomeDaoImpl;
import com.newborntown.dao.impl.DateReportDaoImpl;
import com.newborntown.dao.impl.FbIncomeDaoImpl;
import com.newborntown.dao.impl.InstallCountDaoImpl;
import com.newborntown.dao.impl.PreloadIncomeDaoImpl;
import com.newborntown.dao.impl.RevenueDaoImpl;
import com.newborntown.utils.MongoUtil;

/** 
 * @author chenhao
 * @version 创建时间：2016年11月29日 下午5:46:49 
 */
public class GetUpstreamRevenue {
	
	MongoUtil cm_income_mongo = new MongoUtil(Constant.MONGODB_INCOME_DATABASE, Constant.MONGODB_INCOME_cmIncome,Constant.MOGODB_INCOME_HOST);
	MongoUtil fb_income_mongo = new MongoUtil(Constant.MONGODB_INCOME_DATABASE, Constant.MONGODB_INCOME_fbIncome,Constant.MOGODB_INCOME_HOST);
	MongoUtil preload_income_mongo = new MongoUtil(Constant.MONGODB_INCOME_DATABASE, Constant.MONGODB_INCOME_preloadIncome,Constant.MOGODB_INCOME_HOST);
	MongoUtil install_count_mongo = new MongoUtil(Constant.MONGODB_INCOME_DATABASE, Constant.MONGODB_INCOME_installCount,Constant.MOGODB_INCOME_HOST);//source
	MongoUtil date_report_mongo = new MongoUtil(Constant.MONGODB_DATAREPORT_DATABASE, Constant.MONGODB_DATAREPORT_dataReport,Constant.MOGODB_INCOME_HOST);
	MongoUtil revenue_mongo = new MongoUtil(Constant.MONGODB_INCOME_DATABASE, Constant.MONGODB_INCOME_revenue,Constant.MOGODB_INCOME_HOST);
	
	CmIncomeDao cmDao = new CmIncomeDaoImpl();
	FbIncomeDao fbDao = new FbIncomeDaoImpl();
	PreloadIncomeDao preloadDao = new PreloadIncomeDaoImpl();
	InstallCountDao installCDao = new InstallCountDaoImpl();
	DateReportDao dateRepDao = new DateReportDaoImpl();
	RevenueDao rDAO = new RevenueDaoImpl();
	
	public static void main(String[] args) {
		new GetUpstreamRevenue().InsertAllUpstreamRevenueToMongo("2016:12:04");
	}
	
	/**
	 * 获取当前日期的每个类型的revenue
	 * @param date
	 */
	public void InsertAllUpstreamRevenueToMongo(String date){
		
		date = date.replaceAll("\\-", "\\:");
		
		DecimalFormat df = new DecimalFormat("#0.0000"); //保留几位小数  
		
		double cm_revenue = cmDao.getUpstreamRevenue(cm_income_mongo, date);
		double fb_revenue = fbDao.getUpstreamRevenue(fb_income_mongo, date);
		double preload_revenue = preloadDao.getUpstreamRevenue(preload_income_mongo, date);
		double report_revenue = dateRepDao.getUpstreamRevenue(date_report_mongo, date.replaceAll("\\:", "\\-"));
		
		fb_revenue = fb_revenue-report_revenue;
		
		HashMap<String,Double> install_revenueMap = installCDao.getUpstreamRevenue(install_count_mongo, date);
		
		Document doc = new Document();
		
		if(install_revenueMap.size()>0){
			for (Entry<String, Double> installRevenueDoc : install_revenueMap.entrySet()) {
				
				doc.append(installRevenueDoc.getKey(), Double.parseDouble(df.format(installRevenueDoc.getValue())));
			}
		}

		doc.append("_id", date).append("cm", Double.parseDouble(df.format(cm_revenue))).append("fb", Double.parseDouble(df.format(fb_revenue))).append("shuffle", Double.parseDouble(df.format(preload_revenue))).append("fb_audience", Double.parseDouble(df.format(report_revenue)));
		
		revenue_mongo.singleUpsert(doc);
		
		cm_income_mongo.closeClient();
		fb_income_mongo.closeClient();
		preload_income_mongo.closeClient();
		install_count_mongo.closeClient();
		date_report_mongo.closeClient();
		revenue_mongo.closeClient();
	}
	
	/**
	 * 通过mongo字段计算revenue
	 * @param sourceArray 字段数组
	 * @return
	 */
	public HashMap<String,Double> getUpstreamRevenue(String[] sourceArray,String startDate,String endDate){
		
		HashMap<String,Double> revenueMap = rDAO.getRevenue(revenue_mongo, sourceArray,startDate,endDate);
		
		revenue_mongo.closeClient();
		
		return revenueMap;
		
	}
}
