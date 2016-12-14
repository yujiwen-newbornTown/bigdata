package com.newborntown.dao; 

import java.util.HashMap;

import com.newborntown.utils.MongoUtil;

/** 
 * @author chenhao
 * @version 创建时间：2016年12月2日 上午11:29:49 
 */
public interface UserDao {
	
	public HashMap<String,String> getPublisherIdAndEmail(MongoUtil mongoUtil);

}
