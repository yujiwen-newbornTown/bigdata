package com.newborntown.dao; 

import com.newborntown.utils.MongoUtil;

/** 
 * @author chenhao
 * @version 创建时间：2016年11月30日 上午10:27:32 
 */
public interface DateReportDao {
	
	public double getUpstreamRevenue(MongoUtil mongoUtil,String date);

}
