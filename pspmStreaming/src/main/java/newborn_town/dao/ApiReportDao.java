package newborn_town.dao;

import java.util.ArrayList;
import java.util.IdentityHashMap;

import newborn_town.commonpojo.ReportMongoBean;
import newborn_town.util.MongoUtil;

import org.bson.Document;

/**
 * @author  yujiwen E-mail: yujiwen@newborn-town.com
 * @version 创建时间：2016年11月24日 上午11:13:57
 * 
 */
public interface ApiReportDao {

	public ArrayList<Document> getApiReportDocument(MongoUtil mongoUtil);
	public void insertIntoMongo(IdentityHashMap<String,ReportMongoBean> reportMap,MongoUtil mongoUtil);
}
