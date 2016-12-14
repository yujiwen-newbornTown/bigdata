package com.newborntown.dao; 

import com.newborntown.utils.MongoUtil;

/** 
 * @author chenhao
 * @version 创建时间：2016年11月30日 上午9:54:38 
 */
public interface PreloadIncomeDao {
	
	public double getUpstreamRevenue(MongoUtil mongoUtil,String date);

}
