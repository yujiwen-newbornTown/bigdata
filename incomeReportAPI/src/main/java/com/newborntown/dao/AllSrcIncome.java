package com.newborntown.dao;

import com.newborntown.utils.MongoUtil;

/**
 * @author  yujiwen E-mail: yujiwen@newborn-town.com
 * @version 创建时间：2016年12月5日 下午3:34:33
 * 
 */
public interface AllSrcIncome {
	public void insertPayOut(MongoUtil mongoUtilStatistic,String date);
}
