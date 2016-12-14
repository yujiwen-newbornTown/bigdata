package com.newborntown.dao; 

import java.util.HashMap;

import com.newborntown.utils.MongoUtil;

/** 
 * @author chenhao
 * @version 创建时间：2016年11月30日 上午9:55:03 
 */
public interface InstallCountDao {
	
	public HashMap<String,Double> getUpstreamRevenue(MongoUtil mongoUtil,String date);

}
