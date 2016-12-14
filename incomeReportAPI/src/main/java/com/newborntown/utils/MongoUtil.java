package com.newborntown.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.log4j.Logger;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.ServerAddress;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.DeleteOneModel;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.InsertOneModel;
import com.mongodb.client.model.UpdateOneModel;
import com.mongodb.client.model.UpdateOptions;
import com.mongodb.client.model.WriteModel;
import com.newborntown.constant.Constant;

/**
 * Mongodb工具类
 * 
 * @author yangyang
 *
 */
public class MongoUtil {

	static Logger logger = Logger.getLogger(MongoUtil.class);

	private MongoCollection<Document> collections;

	//private MongoClient client;
	
	private MongoClient client = new MongoClient(Constant.MOGODB_INCOME_HOST);
	
	/**
	 * 
	 * @param dataBase mongodb库名
	 * @param collection  mongodb表名
	 * @param host  mongodb ip地址
	 * @param userNanme mongodb 用户名
	 * @param passWord  mongodb 密码
	 */
	public MongoUtil(String dataBase, String collection , String host,String userNanme ,String passWord) {

		MongoCredential credential = MongoCredential.createCredential(
				userNanme, "admin",
				passWord.toCharArray());// 设置用户认证

		try {

			client = new MongoClient(Arrays.asList(new ServerAddress(
					 host)), Arrays.asList(credential));

			collections = client.getDatabase(dataBase)
					.getCollection(collection);

		} catch (Exception e) {

			logger.error(e.getMessage(),e);
		}
	}
	
	public MongoUtil(String dataBase, String collection , String host) {

		try {

			client = new MongoClient(Arrays.asList(new ServerAddress(host)));
			
			collections = client.getDatabase(dataBase)
					.getCollection(collection);

		} catch (Exception e) {

			logger.error(e.getMessage(),e);
		}
	}
	
	/**
	 * 统计数据条数
	 * @param dataBase
	 * @param collection
	 * @return
	 */
	public long getMongoCount(Document query){
		
		long cleng = 0;
		
		if(null !=query){
			cleng = collections.count(query);
		}else{
			cleng = collections.count();
		}
		return cleng;
	}

	
	/**
	 * 关闭连接
	 */
	public void closeClient() {
		if(client!=null)
			client.close();
	}
	
	/**
	 * 根据id查询数据
	 * 
	 * @param client
	 *            mongodb client
	 * @param dataBase
	 *            mongodb 数据库
	 * @param collection
	 *            collection 表
	 * @param ids
	 *            id集合 如果ids为null 则查询所有数据 如果ids不为null 则查询对应的数据
	 * @return
	 */
	public static FindIterable<Document> getAllDatasByIds(MongoClient client,
			String dataBase, String collection, ArrayList<String> ids) {

		MongoCollection<Document> collections = client.getDatabase(dataBase)
				.getCollection(collection);

		FindIterable<Document> findIterable;

		if (null != ids && ids.size() > 0) {
			findIterable = collections.find(new Document("_id", new Document(
					"$in", ids)));
		} else {
			findIterable = collections.find();
		}

		return findIterable;
	}

	public FindIterable<Document> getAllDatasByIds(ArrayList<String> ids) {

		if (null == collections)
			return null;

		FindIterable<Document> findIterable;

		if (null != ids && ids.size() > 0) {
			findIterable = collections.find(new Document("_id", new Document(
					"$in", ids)));
		} else {
			findIterable = collections.find();
		}

		return findIterable;
	}
	
	
	

	/**
	 * 根据索引id获取一条数据
	 * 
	 * @param client
	 *            mongodb client
	 * @param dataBase
	 *            mongodb 数据库
	 * @param collection
	 *            collection 表
	 * @param id
	 * @return
	 */
	public static FindIterable<Document> getDataById(MongoClient client,
			String dataBase, String collection, String id) {

		MongoCollection<Document> collections = client.getDatabase(dataBase)
				.getCollection(collection);

		FindIterable<Document> findIterable = collections.find(new Document(
				"_id", id));

		return findIterable;
	}

	/**
	 * 根据id 更新某个字段
	 * 
	 * @param client
	 *            mongodb client
	 * @param dataBase
	 *            mongodb 数据库
	 * @param collection
	 *            collection 表
	 * @param id
	 *            索引id
	 * @param key
	 *            字段名
	 * @param value
	 *            值
	 */
	public static void updateDataById(MongoClient client, String dataBase,
			String collection, String id, String key, int value) {

		MongoCollection<Document> collections = client.getDatabase(dataBase)
				.getCollection(collection);
		collections.updateOne(Filters.eq("_id", id), new Document("$set",
				new Document(key, value)), new UpdateOptions().upsert(true));
	}

	/**
	 * 批量更新 字符串
	 * 
	 * @param documents
	 */
	public void bulkWriteUpdateStr(List<Document> documents) {

		if (null == collections || null == documents || documents.size() == 0)
			return;

		List<WriteModel<Document>> requests = new ArrayList<WriteModel<Document>>();

		for (Document document : documents) {

			Document queryDocument = new Document("_id", document.get("_id"));

			document.remove("_id");

			// 更新条件
			Document updateDocument = new Document("$set", document);

			// 构造更新单个文档的操作模型
			UpdateOneModel<Document> uom = new UpdateOneModel<Document>(
					queryDocument, updateDocument,
					new UpdateOptions().upsert(true));
			// UpdateOptions代表批量更新操作未匹配到查询条件时的动作，默认false，什么都不干，true时表示将一个新的Document插入数据库，他是查询部分和更新部分的结合

			requests.add(uom);
		}

		try {

			collections.bulkWrite(requests);
		} catch (Exception e) {
			logger.error("mongodbUtil Exception-----" + e);
		}
	}

	/**
	 * 批量累加
	 * 
	 * @param documents
	 */
	public void bulkWriteUpdateInc(List<Document> documents) {

		if (null == collections || null == documents || documents.size() == 0)
			return;

		List<WriteModel<Document>> requests = new ArrayList<WriteModel<Document>>();

		for (Document document : documents) {

			Document queryDocument = new Document("_id", document.get("_id"));

			document.remove("_id");

			// 更新条件
			Document updateDocument = new Document("$inc", document);

			// 构造更新单个文档的操作模型
			UpdateOneModel<Document> uom = new UpdateOneModel<Document>(
					queryDocument, updateDocument,
					new UpdateOptions().upsert(true));
			// UpdateOptions代表批量更新操作未匹配到查询条件时的动作，默认false，什么都不干，true时表示将一个新的Document插入数据库，他是查询部分和更新部分的结合

			requests.add(uom);
		}

		try {
			collections.bulkWrite(requests);
		} catch (Exception e) {
			logger.error("mongodbUtil Exception-----" + e);
		}
	}

	/**
	 * 批量插入数据
	 * 
	 * @param documents
	 */
	public void bulkWriteInsert(List<Document> documents) {

		if (null == collections || null == documents || documents.size() == 0)
			return;

		List<WriteModel<Document>> requests = new ArrayList<WriteModel<Document>>();

		for (Document document : documents) {
			// 构造插入单个文档的操作模型
			InsertOneModel<Document> iom = new InsertOneModel<Document>(
					document);
			requests.add(iom);
		}
		collections.bulkWrite(requests);
	}
	
	
	/**
	 * 单条插入数据
	 * 
	 * @param documents
	 */
	public void singleInsert(Document document) {

		if (null == collections || null == document)
			return;
		collections.insertOne(document);
	}
	
	public void singleUpsert(Document document) {
		
		if (null == collections || null == document)
			return;
		
		Document queryDocument = new Document("_id", document.get("_id"));

		document.remove("_id");
		// 更新条件
		Document updateDocument = new Document("$set", document);

		collections.updateOne(queryDocument, updateDocument,new UpdateOptions().upsert(true));
		
	}
	

	/**
	 * 批量删除
	 * 
	 * @param documents
	 */
	public void bulkWriteDelete(List<Document> documents) {

		if (null == collections || null == documents || documents.size() == 0)
			return;

		List<WriteModel<Document>> requests = new ArrayList<WriteModel<Document>>();
		for (Document document : documents) {
			// 删除条件
			Document queryDocument = new Document("_id", document.get("_id"));
			// 构造删除单个文档的操作模型，
			DeleteOneModel<Document> dom = new DeleteOneModel<Document>(
					queryDocument);
			requests.add(dom);
		}

		collections.bulkWrite(requests);
	}
	
	/**
	 * 根据条件查询相关字段
	 * @param query 查询条件
	 * @param result 查询结果字段     new Document("name", 1);  该字段 1代表返回该字段
	 * @return
	 */
	public FindIterable<Document> findDatas(Document query , Document result){
		
		 FindIterable<Document> res = null;
		
		if(null != query && null != result){
			
			res  =  collections.find(query).projection(result);
			
		}else if(null == result && null != query){
		
			res = collections.find(query);
			
		}else if(null == query && null != result){
			
			res = collections.find().projection(result);
		
		}else{
			
			res = collections.find();
			
		}
		
		return res;
	}
	
	/**
	 * 根据_id批量获取数据
	 * @param query
	 * @param result
	 * @return
	 */
	public FindIterable<Document> findDatasBatch(ArrayList<String> query , Document result){
		
		 FindIterable<Document> res = null;
		 
		 res =  collections.find(new Document(new Document("_id",new Document("$in",query)))).projection(result);
		
		return res;
	}
	
	
	public void insterDatas(ArrayList<Document> documents) {

		if (null == collections || null == documents || documents.size() == 0) {
			return;
		}

		collections.insertMany(documents);
	}
	
	/**
	 * 更改表和库
	 * @param database
	 * @param collection
	 * @return
	 */
	public void  changeDatabaseTables(String database,String collection){
		collections = client.getDatabase(database).getCollection(collection);
	}
	
	/**
	 * 根据条件删除数据
	 * @param query
	 */
	public void removeDatas(Document query){
		collections.deleteMany(query);
	}
	

	/**
	 * 运行聚合函数
	 * @param pipeline
	 * @return
	 */
	public AggregateIterable<Document> findCollection(List<Document> pipeline) {
		
		AggregateIterable<Document> iterable = collections.aggregate(pipeline).allowDiskUse(true);
		
		return iterable;
	}
	
	
	
}
