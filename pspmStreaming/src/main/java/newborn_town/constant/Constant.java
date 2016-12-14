package newborn_town.constant;

/**
 * @author chenhao
 * @version 创建时间：2016年8月2日 上午10:03:38
 */
public class Constant {

	// 批量提交的大小
	public static final String KAFKA_BATCH_SIZE = "16384";
	// 生产者缓冲大小，当缓冲区耗尽后，额外的发送调用将被阻塞。时间超过max.block.ms将抛出TimeoutException
	public static final String KAFKA_BUFFER_MEMORY = "33554432";

	public static final String KAFKA_SESSION_TIMEOUT = "30000";
	
	public static final String TABLE_UserProfile = "UserProfile";
	public static final String TABLE_Statistics = "IndexStatistics";
	public static final String TABLE_AdCounter = "AdCounter_Test";
	public static final String TABLE_APPInfo = "appInfo";
	public static final String CF_Behavior="Behavior";
	public static final String CF_BasicInfo="BasicInfo";
	public static final String CF_Counter ="Counter";
	public static final String CF_AppInfo ="AppInfo";
	public static final String TOPIC_Install ="install";
	public static final String TOPIC_Click ="impression_01";
	public static final String TOPIC_Impression ="impression";
	public static final String TOPIC_AppList ="appList";

	
	
	//模型数据存放路径
	public static final String MODEL_DATA_PATH = "/usr/jihao/data";
	public static final String STATISTICS_PATH = "/usr/jihao/statistics";
	public static final String STATISTICS_PATH_TMP = "/usr/jihao/statisticstmp";
	public static final String TRAINSAMPLE_PATH = "/usr/jihao/trainsample";
	public static final String ARCHIVE_PATH = "/usr/jihao/archive";
	public static final String L1_MODEL_PATH = "/usr/jihao/L1model";
	public static final String CF_MODEL_PATH = "/usr/jihao/CFmodel";
	public static final String OUTPUT_PATH = "/usr/jihao/output";  
	public static final String APPLIST_PATH = "/usr/yangyang/applist";
	

	
	/*
	//线上常量信息
	public static final String HIVEURL = "jdbc:hive2://192.168.194.4:10000/default;auth=noSasl";
	public static final String ZKHosts ="192.168.214.239:2181,192.168.142.173:2181,192.168.216.252:2181";
	public static final String HBaseRootDir ="hdfs://192.168.194.4:9000/hbase";
	public static final String HBaseZKQuorum ="node1,node2,node3";
	public static final String Redis ="192.168.193.9";
	public static final String FSDefaultFS = "hdfs://node6:9000"; 
	public static final String HBASEZKHosts ="192.168.214.239,192.168.142.173,192.168.216.252";
	public static final String HBASEMASTER ="192.168.194.4:60000";
	public static final String KAFKA_HOST = "node1:9092,node2:9092";
	public static final String MOGODB_HOST = "52.34.116.29:27017";
	public static final String MOGODB_USERNASME = "newborn-town";
	public static final String MOGODB_PASSWORD = "17dMVM0SouA2n37j";
*/
	
	
	
	
	
	//线下常量信息（230）
	public static final String HIVEURL = "jdbc:hive2://192.168.0.233:10000/default";
	public static final String HBaseRootDir ="hdfs://192.168.0.230:9000/hbase";
	public static final String HBaseZKQuorum ="node230,node231,node233";
	public static final String Redis ="192.168.0.231";
	public static final String ZKHosts ="192.168.0.230:2181,192.168.0.231:2181,192.168.0.233:2181";
	public static final String FSDefaultFS = "hdfs://192.168.0.230:9000";  
	public static final String HBASEZKHosts ="192.168.0.230,192.168.0.231,192.168.0.233";
	public static final String HBASEMASTER ="192.168.0.230:60000"; 
	public static final String KAFKA_HOST = "192.168.0.231:9092,192.168.0.232:9092,192.168.0.233:9092,192.168.0.234:9092";
	public static final String MOGODB_HOST = "192.168.0.230:27017";
	public static final String MOGODB_USERNASME = "sys";
	public static final String MOGODB_PASSWORD = "123456"; 
}
