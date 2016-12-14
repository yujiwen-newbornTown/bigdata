package newborn_town.dao;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.Map;

import newborn_town.commonpojo.ConversionReportMongoBean;
import newborn_town.commonpojo.ReportMongoBean;
import newborn_town.util.MongoUtil;

import org.bson.Document;

/**
 * @author  yujiwen E-mail: yujiwen@newborn-town.com
 * @version 创建时间：2016年11月24日 下午3:38:25
 * 
 */
public interface ConversionBaseReportDao {
	public ArrayList<Document> getApiReportDocument(MongoUtil mongoUtil);
	public void insertIntoMongo(IdentityHashMap<String, ConversionReportMongoBean> baseUpdateMap,MongoUtil mongoUtil);
	
}
