package com.newborntown.dao; 

import com.newborntown.utils.MongoUtil;

/** 
 * @author chenhao
 * @version 创建时间：2016年11月30日 上午9:53:45 
 */
public interface CmIncomeDao {
	
	public double getUpstreamRevenue(MongoUtil mongoUtil,String date);

}
