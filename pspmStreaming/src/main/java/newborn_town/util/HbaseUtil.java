package newborn_town.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import newborn_town.constant.Constant;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;


public class HbaseUtil {
	static Logger logger = Logger.getLogger(HbaseUtil.class);
	private static Configuration conf = null;
	private Connection connection = null;

	static {
		conf = HBaseConfiguration.create();
		conf.set("hbase.zookeeper.quorum", Constant.HBaseZKQuorum);
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		conf.set("hbase.rootdir", Constant.HBaseRootDir);
		conf.set("hbase.client.write.buffer", "5000000");
		conf.set("hbase.rpc.timeout", "600000");

	}

	public Connection getConnecton() {
		if (connection != null)
			return connection;
		try {
			connection = ConnectionFactory.createConnection(conf);
		} catch (IOException e) {
			connection = null;
			logger.error("connection create exception: " + e.getMessage());
		}
		return connection;
	}

	public void closeConnection() {
		if (connection != null) {
			try {
				connection.close();
			} catch (IOException e) {
				logger.error("connection close error:" + e.getMessage());
			}
			connection = null;
		}
	}

	/**
	 * 创建表
	 * 
	 * @param tablename
	 *            表名
	 * @param cfs
	 *            列簇
	 */
	public void createTable(String tablename, String[] cfs) {
		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return;
		}
		try {
			Admin admin = connection.getAdmin();
			if (admin.tableExists(TableName.valueOf(tablename))) {
				logger.info("table " + tablename + " already exists!");
			} else {
				HTableDescriptor tableDesc = new HTableDescriptor(
						TableName.valueOf(tablename));
				for (int i = 0; i < cfs.length; i++) {
					tableDesc.addFamily(new HColumnDescriptor(cfs[i]));
				}
				admin.createTable(tableDesc);
				admin.close();
				logger.info("create table " + tablename + " ok.");
			}
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 创建一个与分区表
	 * 
	 * @param tablename
	 *            表名
	 * @param cfs
	 *            列族
	 * @param splitkeys
	 *            与分区key
	 */
	public void createTable(String tablename, String[] cfs, byte[][] splitkeys) {
		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return;
		}
		try {
			Admin admin = connection.getAdmin();
			if (admin.tableExists(TableName.valueOf(tablename))) {
				logger.info("table " + tablename + " already exists!");
			} else {
				HTableDescriptor tableDesc = new HTableDescriptor(
						TableName.valueOf(tablename));
				for (int i = 0; i < cfs.length; i++) {
					tableDesc.addFamily(new HColumnDescriptor(cfs[i]));
				}
				admin.createTable(tableDesc, splitkeys);
				admin.close();
				logger.info("create table " + tablename + " ok.");
			}
		} catch (MasterNotRunningException e) {
			e.printStackTrace();
		} catch (ZooKeeperConnectionException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 删除表
	 * 
	 * @param tablename
	 *            表名
	 */
	public void deleteTable(String tablename) {
		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return;
		}
		try {
			Admin admin = connection.getAdmin();
			admin.disableTable(TableName.valueOf(tablename));
			admin.deleteTable(TableName.valueOf(tablename));
			logger.info("delete table " + tablename + " ok.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void truncateTable(String tablename, boolean preserveSplits) {
		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return;
		}
		try {
			Admin admin = connection.getAdmin();
			admin.disableTable(TableName.valueOf(tablename));
			admin.truncateTable(TableName.valueOf(tablename), preserveSplits);
			logger.info("truncate table " + tablename + " ok.");
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * �h���Y��
	 * 
	 * @param tableName
	 * @param rowKey
	 */
//	public void delRecords(String tableName, ArrayList<HBaseCellBean> cellArray) {
//		if (getConnecton() == null) {
//			logger.error("connect hbase error, connection is null");
//			return;
//		}
//
//		Table table = null;
//		try {
//			table = connection.getTable(TableName.valueOf(tableName));
//		} catch (IOException e) {
//			e.printStackTrace();
//			return;
//		}
//
//		for (HBaseCellBean cell : cellArray) {
//			if (cell.getRowKey() == null)
//				continue;
//			Delete delete = new Delete(cell.getRowKey());
//
//			if (cell.getQualifier() != null && cell.getFamily() != null) {
//				// ɾ����
//				delete.deleteColumn(cell.getFamily(), cell.getQualifier());
//			} else if (cell.getFamily() != null) {
//				// ɾ������
//				delete.deleteFamily(cell.getFamily());
//			}
//
//			try {
//				table.delete(delete);
//			} catch (IOException e) {
//				logger.error("del record " + Bytes.toString(cell.getRowKey())
//						+ " to table " + tableName + " "
//						+ Bytes.toString(cell.getFamily()) + ":"
//						+ Bytes.toString(cell.getQualifier()) + "exception:"
//						+ e.getMessage());
//				continue;
//			}
//			logger.info("del record " + Bytes.toString(cell.getRowKey())
//					+ " to table " + tableName + " "
//					+ Bytes.toString(cell.getFamily()) + ":"
//					+ Bytes.toString(cell.getQualifier()));
//
//		}
//		try {
//			table.close();
//		} catch (IOException e) {
//			e.printStackTrace();
//			return;
//		}
//	}

	/**
	 * 删除指定rowkey的cell信息
	 * @param tableName
	 * @param cellArray
	 */
	public void delRecordsByRowkeyCell(String tableName, byte[] family, HashMap<byte[],HashMap<byte[],byte[]>> delByteMap) {
		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return;
		}

		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		for (Map.Entry<byte[],HashMap<byte[], byte[]>> entity : delByteMap.entrySet()) {
			
			HashMap<byte[], byte[]> delColumns = entity.getValue();
			if(entity.getKey() == null){
				continue;
			}
			Delete delete = new Delete(entity.getKey());
			for(byte[] col : delColumns.keySet()){
				delete.addColumns(family, col);
			}
			
			try {
				table.delete(delete);
			} catch (IOException e) {
				
				continue;
			}
		
		}
		try {
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
	}
	
	/**
	 * 判断rowkey是否存在某个区间范围
	 * 
	 * @param tableName
	 *            表名
	 * @param rowKey
	 *            rowkey
	 * @param timestampstart
	 *            起止时间
	 * @param timestampend
	 *            终止时间
	 * @return
	 */
	public boolean rowExist(String tableName, byte[] rowKey,
			long timestampstart, long timestampend) {
		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return false;
		}

		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}

		boolean r = false;
		try {
			Get get = new Get(rowKey);
			get.setTimeRange(timestampstart, timestampend);
			r = table.exists(get);
			table.close();
		} catch (IOException e) {
			logger.error("rowExist exception:" + e.getMessage());
			return false;
		}
		return r;
	}

	/**
	 * 根据rowkey获取一条数据
	 * 
	 * @param tableName
	 *            表名
	 * @param rowKey
	 *            rowkey
	 */
	public Result getRowRecord(String tableName, byte[] rowKey) {
		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return null;
		}

		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		Get get = new Get(rowKey);
		Result r = null;
		try {
			if (!table.exists(get))
				return null;
			r = table.get(get);
			table.close();
		} catch (IOException e) {
			logger.error("get record exception:" + e.getMessage());
			return null;
		}

		return r;
	}

	/**
	 * 根据rowkey批量获取数据
	 * 
	 * @param tableName
	 *            表名
	 * @param rowkeys
	 *            rowkey集合
	 * @return
	 */
	public Result[] getRowsRecord(String tableName, List<byte[]> rowkeys) {
		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return null;
		}

		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		List<Get> gets = new ArrayList<Get>();

		for (byte[] rowKey : rowkeys) {

			if (null != rowkeys && rowKey.length > 0) {
				Get get = new Get(rowKey);
				gets.add(get);
			}
		}

		Result[] r = null;
		try {
			r = table.get(gets);
			table.close();
		} catch (IOException e) {
			logger.error("get record exception:" + e.getMessage());
			return null;
		}
		return r;
	}

	/**
	 * 批量获取某个列族的数据
	 * 
	 * @param tableName
	 *            表明
	 * @param rowkeys
	 *            rowkey集合
	 * @param cf
	 *            列簇名
	 * @return
	 */
	public Result[] getColumns(String tableName, List<String> rowkeys, byte[] cf) {
		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return null;
		}

		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		List<Get> gets = new ArrayList<Get>();
		for (String rowKey : rowkeys) {
			Get get = new Get(Bytes.toBytes(rowKey));
			get.addFamily(cf);
			gets.add(get);
		}

		Result[] r = null;
		try {

			r = table.get(gets);
			table.close();
		} catch (IOException e) {
			logger.error("get record exception:" + e.getMessage());
			return null;
		}

		return r;
	}

	public Result getRowRecord(String tableName, byte[] rowKey,
			long timestampstart, long timestampend) {
		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return null;
		}

		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			logger.error("get record exception:" + e.getMessage());
			return null;
		}

		Result r = null;
		try {
			Get get = new Get(rowKey);
			get.setTimeRange(timestampstart, timestampend);
			if (!table.exists(get))
				return null;
			r = table.get(get);
			table.close();
		} catch (IOException e) {
			logger.error("get record exception:" + e.getMessage());
			return null;
		}

		return r;
	}

	public byte[] getColumnValue(String tableName, byte[] rowKey, byte[] cf,
			byte[] qualifier, long timestampstart, long timestampend) {
		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return null;
		}

		if (cf == null || qualifier == null)
			return null;

		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			logger.error("getColumnValue exception:" + e.getMessage());
			return null;
		}

		Result rs = null;
		try {
			Get get = new Get(rowKey);
			get.setTimeRange(timestampstart, timestampend);
			rs = table.get(get);
			table.close();
		} catch (IOException e) {
			logger.error("get record exception:" + e.getMessage());
			return null;
		}
		if (rs == null)
			return null;
		return rs.getValue(cf, qualifier);
	}

	public byte[] getColumnValue(String tableName, byte[] rowKey, byte[] cf,
			byte[] qualifier) {
		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return null;
		}

		if (cf == null || qualifier == null)
			return null;

		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		Get get = new Get(rowKey);

		Result rs = null;
		try {
			rs = table.get(get);
			table.close();
		} catch (IOException e) {
			logger.error("get record exception:" + e.getMessage());
			return null;
		}
		if (rs == null)
			return null;
		return rs.getValue(cf, qualifier);
	}

	public boolean setColumnValue(String tableName, byte[] rowKey, byte[] cf,
			byte[] qualifier, byte[] value) {
		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return false;
		}

		if (rowKey == null || cf == null || qualifier == null || value == null)
			return false;

		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));
			Put put = new Put(rowKey);
			put.addColumn(cf, qualifier, value);
			table.put(put);
			table.close();
		} catch (IOException e) {
			logger.error("put record exception:" + e.getMessage());
			return false;
		}

		return true;
	}

	public ResultScanner getResultScanner(String tableName) {
		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return null;
		}

		Scan scan = new Scan();
		ResultScanner rs = null;
		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));
			rs = table.getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return rs;
	}

	public boolean rowExist(String tableName, byte[] rowKey) {
		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return false;
		}

		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		Get get = new Get(rowKey);

		boolean r = false;
		try {
			r = table.exists(get);
			table.close();
		} catch (IOException e) {
			logger.error("rowExist exception:" + e.getMessage());
			return false;
		}
		return r;
	}

	/**
	 * 插入一条数据到hbase表
	 * 
	 * @param tableName 表名
	 * @param rowKey   主键
	 * @param family   列族
	 * @param column   列名
	 * @param value    值
	 */
	public void insertData(String tableName, byte[] rowKey, byte[] family,
			byte[] column, byte[] value) {

		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return;
		}

		Table table = null;

		try {
			table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		Put put = new Put(rowKey);// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值

		put.addColumn(family, column, value);

		try {
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * 插入一条数据到hbase表
	 * 
	 * @param tableName
	 *            表名
	 * @param rowKey
	 *            rowkey
	 * @param family
	 *            列簇
	 * @param columnsAndValues
	 *            列名以及对应的值
	 */
	public void insertData(String tableName, byte[] rowKey, byte[] family,
			HashMap<byte[], byte[]> columnsAndValues) {

		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return;
		}

		Table table = null;

		try {
			table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		Put put = new Put(rowKey);// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值

		for (Map.Entry<byte[], byte[]> entry : columnsAndValues.entrySet()) {
			put.addColumn(family, entry.getKey(), entry.getValue());
		}

		try {
			table.put(put);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 批量插入hbase表中数据
	 * 
	 * @param tableName
	 *            表名
	 * @param family
	 *            列族
	 * @param rcvalues
	 *            HashMap<rowkey, HashMap<cf, value>>
	 */
	public void insertDatas(String tableName, byte[] family,
			HashMap<byte[], HashMap<byte[], byte[]>> rcvalues) {

		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return;
		}

		Table table = null;

		try {
			table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		Set<byte[]> rowkeys = rcvalues.keySet();

		List<Put> puts = new ArrayList<Put>();

		for (byte[] rowkey : rowkeys) {

			Put put = new Put(rowkey);// 一个PUT代表一行数据，再NEW一个PUT表示第二行数据,每行一个唯一的ROWKEY，此处rowkey为put构造方法中传入的值

			for (Map.Entry<byte[], byte[]> entry : rcvalues.get(rowkey)
					.entrySet()) {
				put.addColumn(family, entry.getKey(), entry.getValue());
			}

			puts.add(put);
		}

		try {
			table.put(puts);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	/**
	 * 判断rokey,以及某个列名是否存在
	 * 
	 * @param tableName
	 *            表名
	 * @param rowKey
	 *            rowkey
	 * @param columns
	 *            列名
	 * @return
	 */
	public boolean rowAndColumnsExist(String tableName, byte[] rowKey,
			byte[] family, ArrayList<byte[]> columns) {
		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return false;
		}

		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}

		Get get = new Get(rowKey);

		for (byte[] column : columns) {
			get.addColumn(family, column);
		}

		boolean r = false;
		try {
			r = table.exists(get);
			table.close();
		} catch (IOException e) {
			logger.error("rowExist exception:" + e.getMessage());
			return false;
		}
		return r;
	}

	/**
	 * 批量判断rowkey,以及某个列名是否存在
	 * 
	 * @param tableName
	 *            表名
	 * @param rowKeys
	 *            rowKey集合
	 * @return 存在返回ture 不存在返回false
	 */
	public boolean[] rowsExist(String tableName, ArrayList<byte[]> rowKeys) {

		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return null;
		}

		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

		List<Get> gets = new ArrayList<Get>();

		for (byte[] rowkey : rowKeys) {

			Get get = new Get(rowkey);
			gets.add(get);
		}

		boolean[] r = null;
		try {
			r = table.existsAll(gets);
			table.close();
		} catch (IOException e) {
			logger.error("rowExist exception:" + e.getMessage());
			return r;
		}
		return r;
	}

	/**
	 * 批量判断rokey,以及某个列名是否存在
	 * 
	 * @param tableName
	 *            表名
	 * @param rowKeys
	 *            rowKey集合
	 * @param family
	 *            列簇
	 * @param columns
	 *            列名集合
	 * @return
	 */
	public boolean[] rowsAndColumnsExist(String tableName,
			ArrayList<byte[]> rowKeys, byte[] family, ArrayList<byte[]> columns) {
		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return null;
		}

		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

		List<Get> gets = new ArrayList<Get>();

		for (byte[] rowkey : rowKeys) {

			Get get = new Get(rowkey);

			for (byte[] column : columns) {

				get.addColumn(family, column);
			}
			gets.add(get);
		}

		boolean[] r = null;
		try {
			r = table.existsAll(gets);
			table.close();
		} catch (IOException e) {
			logger.error("rowExist exception:" + e.getMessage());
			return r;
		}
		return r;
	}

	/*
	 * 根据rowlist和column，判断rowlist中存在的rows
	 */
	public boolean[] rowsAndColumnExist(String tableName,
			ArrayList<byte[]> rowKeys, byte[] family, byte[] column) {
		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return null;
		}

		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

		List<Get> gets = new ArrayList<Get>();

		for (byte[] rowkey : rowKeys) {

			Get get = new Get(rowkey);
			get.addColumn(family, column);
			gets.add(get);

		}

		boolean[] r = null;
		try {
			r = table.existsAll(gets);
			table.close();
		} catch (IOException e) {
			logger.error("rowExist exception:" + e.getMessage());
			return r;
		}
		return r;
	}

	/**
	 * 批量获取某个列的值
	 * 
	 * @param tableName
	 *            表名
	 * @param rowkeys
	 *            rowkey集合
	 * @param family
	 *            列簇
	 * @param Columns
	 *            列名集合
	 * @return
	 */
	public Result[] getRowsRecordByColumns(String tableName,
			ArrayList<byte[]> rowkeys, byte[] family, ArrayList<byte[]> columns) {

		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return null;
		}

		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}
		List<Get> gets = new ArrayList<Get>();

		for (byte[] rowKey : rowkeys) {

			Get get = new Get(rowKey);

			for (byte[] column : columns) {
				get.addColumn(family, column);
			}
			gets.add(get);
		}

		Result[] r = null;
		try {
			r = table.get(gets);
			table.close();
		} catch (IOException e) {
			logger.error("get record exception:" + e.getMessage());
			return null;
		}
		return r;
	}

	/**
	 * 批量获取某个列的值根据rowkey
	 * 
	 * @param tableName
	 *            表名
	 * @param family
	 *            列族
	 * @param rowkeysAndColumns
	 *            HashMap<rowkey , 列名集合>
	 * @return
	 */
	public Result[] getRowsRecordByColumns(String tableName, byte[] family,
			HashMap<byte[], ArrayList<byte[]>> rowkeysAndColumns) {

		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return null;
		}

		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

		List<Get> gets = new ArrayList<Get>();

		Set<byte[]> rowkeys = rowkeysAndColumns.keySet();

		for (byte[] rowkey : rowkeys) {

			Get get = new Get(rowkey);

			ArrayList<byte[]> columns = rowkeysAndColumns.get(rowkey);

			for (byte[] column : columns) {
				get.addColumn(family, column);
			}
			gets.add(get);
		}

		Result[] r = null;
		try {
			r = table.get(gets);
			table.close();
		} catch (IOException e) {
			logger.error("get record exception:" + e.getMessage());
			return null;
		}
		return r;
	}

	/**
	 * 获取某个列的值根据rowkey
	 * 
	 * @param tableName
	 * @param family
	 * @param rowkey
	 *            rowkey
	 * @param columns
	 *            需要过滤的列
	 * @return
	 */
	public Result getRowRecordByColumns(String tableName, byte[] rowkey,
			byte[] family, ArrayList<byte[]> columns) {

		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return null;
		}

		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
			return null;
		}

		Get get = new Get(rowkey);

		for (byte[] column : columns) {
			get.addColumn(family, column);
		}

		Result r = null;
		try {
			r = table.get(get);
			table.close();
		} catch (IOException e) {
			logger.error("get record exception:" + e.getMessage());
			return null;
		}
		return r;
	}

	/**
	 * 获得某一段rowkey的数据
	 * 
	 * @param tableName
	 *            表名
	 * @param startTime
	 *            起始rowkey
	 * @param endTime
	 *            结束rowkey
	 * @return
	 */
	public ResultScanner getResultScanner(String tableName, String startRowkey,
			String endRowkey) {

		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return null;
		}

		Scan scan = new Scan();
		scan.setStartRow(Bytes.toBytes(startRowkey));
		scan.setStopRow(Bytes.toBytes(endRowkey));
		ResultScanner rs = null;
		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));
			rs = table.getScanner(scan);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return rs;
	}

	/**
	 * 根据起止rowkey批量删除数据
	 * 
	 * @param tableName
	 *            表名
	 * @param startRowkey
	 *            起始rowkey
	 * @param endRowkey
	 *            终止rowkey
	 * @return
	 */
	public boolean deleteRecordsByRowkeys(String tableName, String startRowkey,
			String endRowkey) {

		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return false;
		}

		Table table = null;

		try {
			table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			logger.error("deleteRecordsByRowkeys Excption:" + e);
			return false;
		}

		ResultScanner rs = this.getResultScanner(tableName, startRowkey,
				endRowkey);

		if (null == rs)
			return false;

		List<Delete> list = new ArrayList<Delete>();

		for (Result r : rs) {
			Delete delete = new Delete(r.getRow());
			list.add(delete);
		}

		try {
			table.delete(list);
			return true;
		} catch (IOException e) {
			logger.error("deleteRecordsByRowkeys Excption:" + e);
			return false;
		}
	}

	/**
	 * 根据rowkey批量删除表中数据
	 * 
	 * @param tableName
	 *            表名
	 * @param rowkeys
	 *            rowkey集合
	 * @return
	 */
	public boolean deleteRecordsByRowkeys(String tableName,
			ArrayList<byte[]> rowkeys) {

		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return false;
		}

		Table table = null;

		try {
			table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			logger.error("deleteRecordsByRowkeys Excption:" + e);
			return false;
		}

		List<Delete> list = new ArrayList<Delete>();

		for (byte[] rowkey : rowkeys) {
			Delete delete = new Delete(rowkey);
			list.add(delete);
		}

		try {
			table.delete(list);
			return true;
		} catch (IOException e) {
			logger.error("deleteRecordsByRowkeys Excption:" + e);
			return false;
		}
	}

	/**
	 * 添加HashMap中的记录
	 * 
	 * @param tableName
	 *            表名
	 * @param family
	 *            列簇
	 * @param hm
	 *            需要添加的列名以及对应的值的集合 HashMap<byte[], HashMap<byte[], byte[]>>
	 *            rowkey： rowkey HashMap<byte[], byte[]> rowkey对应的列名以及值
	 * @return
	 */
	public boolean addMapRecords(String tableName, byte[] family,
			HashMap<byte[], HashMap<byte[], byte[]>> hm) {

		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return false;
		}

		List<Put> puts = new ArrayList<Put>();

		Table table = null;

		try {

			table = connection.getTable(TableName.valueOf(tableName));

		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}

		Set<byte[]> rowkeys = hm.keySet();

		HashMap<byte[], byte[]> cfAndValues = new HashMap<byte[], byte[]>();

		for (byte[] rowkey : rowkeys) {

			Put put = new Put(rowkey);

			cfAndValues = hm.get(rowkey);

			Set<byte[]> cfs = cfAndValues.keySet();

			for (byte[] cf : cfs) {
				put.addColumn(family, cf, cfAndValues.get(cf));
			}
			puts.add(put);
		}
	
		try {
			table.put(puts);
		} catch (IOException e1) {
			e1.printStackTrace();
			return false;
		}
		try {
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	/**
	 * 增量数据
	 * 
	 * @param tableName
	 *            表名
	 * @param family
	 *            列簇
	 * @param incrMap
	 *            需要添加的列名以及对应的值的集合 HashMap<byte[], HashMap<byte[], Long>>
	 *            rowkey： rowkey HashMap<byte[], Long> rowkey对应的列名以及增量值
	 * @return
	 */

	public boolean IncrRecordsByColumns(String tableName, byte[] family,
			HashMap<byte[], HashMap<byte[], Long>> incrMap){
		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return false;
		}
		Table table = null;
		try {

			table = connection.getTable(TableName.valueOf(tableName));

		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		
		Set<byte[]> rowkeys = incrMap.keySet();

		for (byte[] rowkey : rowkeys) {

			Increment incr = new Increment(rowkey);

			for (Map.Entry<byte[], Long> entry : incrMap.get(rowkey)
					.entrySet()) {
				incr.addColumn(family, entry.getKey(), entry.getValue());
			}

			try {
				table.increment(incr);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		
		try {
			table.close();
		} catch (IOException e) {
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	/**
	 * 批量更新rowkey信息,固定列名列值
	 * @param tableName 表名
	 * @param family 列簇
	 * @param list 批量更新的rowkey列表
	 * @param column  固定列名
	 * @param value   固定列值
	 */
	public void insertRowFixedColumnValue(String tableName,byte[] family,ArrayList<byte[]> list,byte[] column,byte[] value){
		ArrayList<Put> put_list = new ArrayList<Put>();
		if (getConnecton() == null) {
			logger.error("connect hbase error, connection is null");
			return;
		}

		Table table = null;
		try {
			table = connection.getTable(TableName.valueOf(tableName));
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}
		
		for(byte[] rowkey : list){
			Put put = new Put(rowkey);
			put.add(family, column, value);
			put_list.add(put);
		}
		try {
			table.put(put_list);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * 批量更新rowkey 列数据
	 * @param tablename 表名
	 * @param CF_NAME 列簇
	 * @param hm  插入的HashMap信息
	 * @param hbaseClient 
	 */
	public void batchIncrementRowsMap(String tablename, byte[] CF_NAME, HashMap<String, HashMap<String, Long>> hm, HbaseUtil hbaseClient) {
		
		if ( hm.isEmpty() || hm.size() == 0){
			return;
		}
		HashMap<byte[], HashMap<byte[], byte[]>> map = PublicUtill
				.mapToBytes(hm);

        if(map == null || map.size() == 0)
        	return;
        
        HashMap<byte[] , ArrayList<byte[]>> rowkeysAndColumns = new HashMap<byte[] , ArrayList<byte[]>>();
        for(Entry<byte[], HashMap<byte[], byte[]>> entry : map.entrySet()){
        	HashMap<byte[], byte[]> innerMap = entry.getValue();
        	ArrayList<byte[]> columnList = new ArrayList<byte[]>();
        	for(Entry<byte[], byte[]> innerEntry : innerMap.entrySet()){
        		columnList.add(innerEntry.getKey());
        	}
        	rowkeysAndColumns.put(entry.getKey(), columnList);
        }

		Result[] results = hbaseClient.getRowsRecordByColumns(tablename, CF_NAME, rowkeysAndColumns);

		for (Result result : results) {
			
			if (!result.isEmpty()) {

				String rowkey = Bytes.toString(result.getRow());

				HashMap<String, Long> columnValues_hm = hm.get(rowkey);
				Set<String> columns_sets = columnValues_hm.keySet();

				byte[] value = null;
				
				HashMap<byte[], byte[]> mapByte = new HashMap<byte[], byte[]>();
				for (String column : columns_sets) {

					if (StringUtils.isEmpty(column))
						continue;
					value = result.getValue(CF_NAME, Bytes.toBytes(column));
					if (null == value)
						continue;


					long outValue = Long.parseLong(Bytes.toString(value))
							+ columnValues_hm.get(column); // 需要存入hbase的值（原hbase的值加上现有的值的和）

					mapByte.put(Bytes.toBytes(column), Bytes.toBytes(String.valueOf(outValue)));
					
				}
				map.put(result.getRow(), mapByte);
			}
			
		}
		hbaseClient.addMapRecords(tablename, CF_NAME, map);
	}
	
	public static void main(String[] args) {
		try {

			HbaseUtil hbclient = new HbaseUtil();
			
			String tablename = "appHistory_online";
			// String[] familys = {"info"};
			String[] familys = { "UserInfo" };

			// System.out.println(hbclient.deleteRecordsByRowkeys(tablename,
			// "1", "3"));
			 hbclient.createTable(tablename, familys);
			// hbclient.deleteTable(tablename);

			/*ArrayList<byte[]> rowkeys = new ArrayList<byte[]>();

			rowkeys.add(Bytes.toBytes("ADW.Pink.Abstract"));
			rowkeys.add(Bytes.toBytes("ADW.Yellow.Pulse"));

			hbclient.deleteRecordsByRowkeys("appInfo", rowkeys);*/
			/*
			 * ArrayList<byte[]> rk = new ArrayList<byte[]>();
			 * rk.add("1".getBytes()); rk.add("2".getBytes());
			 * rk.add("3".getBytes());
			 */

			// HashSet<String> hs =
			// hbclient.rowsAndColumnExist("Associated_Ad_Log",rk,"info".getBytes(),"impression_time".getBytes());
			// System.out.println(hs.size()+" "+hs.toString());

			/*
			 * byte[] family = Bytes.toBytes("info");
			 * 
			 * HashMap<byte[],HashMap<byte[], byte[]>> map = new HashMap<byte[],
			 * HashMap<byte[],byte[]>>();
			 * 
			 * HashMap<byte[], byte[]> columnsAndValues = new HashMap<byte[],
			 * byte[]>(); columnsAndValues.put(Bytes.toBytes("nihao"),
			 * Bytes.toBytes("nihao3"));
			 * columnsAndValues.put(Bytes.toBytes("app_history"),
			 * Bytes.toBytes("app_history3:1,app_history3:2,app_history3:3"));
			 * HashMap<byte[], byte[]> columnsAndValues1 = new HashMap<byte[],
			 * byte[]>(); columnsAndValues1.put(Bytes.toBytes("nihao"),
			 * Bytes.toBytes("nihao4"));
			 * columnsAndValues1.put(Bytes.toBytes("app_history"),
			 * Bytes.toBytes("app_history4:1,app_history4:2,app_history4:3"));
			 * 
			 * map.put(Bytes.toBytes("3"), columnsAndValues);
			 * map.put(Bytes.toBytes("4"), columnsAndValues1);
			 * hbclient.insertDatas(tablename, family, map);
			 */

			/*
			 * byte[] rowkey =Bytes.toBytes("2");
			 * 
			 * HashMap<byte[], byte[]> columnsAndValues = new HashMap<byte[],
			 * byte[]>();
			 * 
			 * columnsAndValues.put(Bytes.toBytes("nihao"),
			 * Bytes.toBytes("nihao2"));
			 * columnsAndValues.put(Bytes.toBytes("app_history"),
			 * Bytes.toBytes("app_history2:1,app_history2:2,app_history2:3"));
			 * 
			 * hbclient.insertData(tablename, rowkey, family, columnsAndValues);
			 */

			/*
			 * ArrayList<byte[]> rowkeys = new ArrayList<byte[]>();
			 * rowkeys.add(Bytes.toBytes("03f902b7af241408"));
			 * rowkeys.add(Bytes.toBytes("100062b47b2734dd")); for(byte[] rowkey
			 * : rowkeys){ HBaseCellBean cell = null; cell = new
			 * HBaseCellBean(); cell.setCellEntity(rowkey,
			 * Bytes.toBytes("info"),
			 * Bytes.toBytes("AIM"+":"+"com.morecast.weather"),
			 * Bytes.toBytes("1")); cellArray.add(cell);
			 * 
			 * cell = new HBaseCellBean(); cell.setCellEntity(rowkey,
			 * Bytes.toBytes("info"),
			 * Bytes.toBytes("APIM"+":"+"com.morecast.weather"+":"+"1045"),
			 * Bytes.toBytes("2")); cellArray.add(cell);
			 * 
			 * cell = new HBaseCellBean(); cell.setCellEntity(rowkey,
			 * Bytes.toBytes("info"), Bytes.toBytes("ASIM"+":"+
			 * "com.morecast.weather" +":"+ "1784"), Bytes.toBytes("3"));
			 * cellArray.add(cell);
			 * 
			 * cell = new HBaseCellBean(); cell.setCellEntity(rowkey,
			 * Bytes.toBytes("info"), Bytes.toBytes("ACL"+":"+
			 * "com.morecast.weather"), Bytes.toBytes("4"));
			 * cellArray.add(cell);
			 * 
			 * cell = new HBaseCellBean(); cell.setCellEntity(rowkey,
			 * Bytes.toBytes("info"), Bytes.toBytes("APCL"+":"+
			 * "com.morecast.weather" + ":" +"1045"), Bytes.toBytes("5"));
			 * cellArray.add(cell);
			 * 
			 * cell = new HBaseCellBean(); cell.setCellEntity(rowkey,
			 * Bytes.toBytes("info"), Bytes.toBytes("ASCL"+":"+
			 * "com.morecast.weather" + ":" +"1784"), Bytes.toBytes("6"));
			 * cellArray.add(cell);
			 * 
			 * cell = new HBaseCellBean(); cell.setCellEntity(rowkey,
			 * Bytes.toBytes("info"), Bytes.toBytes("AIS"+":"+
			 * "com.morecast.weather"), Bytes.toBytes("7"));
			 * cellArray.add(cell);
			 * 
			 * cell = new HBaseCellBean(); cell.setCellEntity(rowkey,
			 * Bytes.toBytes("info"), Bytes.toBytes("APIS"+":"+
			 * "com.morecast.weather" + ":" + "1045"), Bytes.toBytes("8"));
			 * cellArray.add(cell);
			 * 
			 * cell = new HBaseCellBean(); cell.setCellEntity(rowkey,
			 * Bytes.toBytes("info"), Bytes.toBytes("ASIS"+":"+
			 * "com.morecast.weather" + ":" + "1784"), Bytes.toBytes("9"));
			 * cellArray.add(cell); }
			 * 
			 * hbclient.addRecords(tablename, cellArray);
			 */

			/*
			 * byte[] CF_NAME = Bytes.toBytes("AppInfo"); // 列族
			 * ArrayList<byte[]> cfs = new ArrayList<byte[]>();
			 * cfs.add(Bytes.toBytes("app_category"));
			 * cfs.add(Bytes.toBytes("111"));
			 * 
			 * System.out.println(hbclient.rowAndColumnsExist(tablename,
			 * Bytes.toBytes("ADW.Pink.Abstract"), CF_NAME, cfs));
			 */

			/*
			 * ArrayList<byte[]> rowkeys = new ArrayList<byte[]>();
			 * 
			 * 
			 * rowkeys.add(Bytes.toBytes("ADW.Pink.Abstract"));
			 * rowkeys.add(Bytes.toBytes("ADW.Yellow.Pulse"));
			 * rowkeys.add(Bytes.toBytes("AKnght.Studios.AngryBricks"));
			 * 
			 * cfs.add(Bytes.toBytes("app_name"));
			 * cfs.add(Bytes.toBytes("app_rating"));
			 * cfs.add(Bytes.toBytes("app_provider"));
			 * 
			 * ArrayList<AppBean> apps = new ArrayList<AppBean>();
			 * 
			 * Result[] results = hbclient.getRowsRecordByColumns(tablename,
			 * rowkeys, CF_NAME, cfs);
			 */

			// Result[] results = hbclient.getRowsRecord(tablename, rowkeys);

			/*
			 * for (Result result : results) {
			 * 
			 * AppBean a = new AppBean();
			 * 
			 * if (result == null) continue;
			 * 
			 * byte[] cellByte = null; cellByte = result.getValue(CF_NAME,
			 * Bytes.toBytes("app_name")); if (cellByte != null)
			 * a.setApp_name(Bytes.toString(cellByte));
			 * 
			 * cellByte = result .getValue(CF_NAME,
			 * Bytes.toBytes("app_rating")); if (cellByte != null)
			 * a.setApp_rating(Double.parseDouble(Bytes.toString(cellByte)));
			 * cellByte = result.getValue(CF_NAME,
			 * Bytes.toBytes("app_provider")); if (cellByte != null)
			 * a.setApp_provider(Bytes.toString(cellByte)); apps.add(a); }
			 */

			/*
			 * String rowkey = "20160614103920:1";
			 * 
			 * byte[] CF_NAME = Bytes.toBytes("AppInfo"); //列族 byte[]
			 * CATEGORY_COLUMN_NAME = Bytes.toBytes("app_category");//app的类别
			 */
			/*
			 * ArrayList<HBaseCellBean> cellArray = new
			 * ArrayList<HBaseCellBean>(); HBaseCellBean cell = null; //category
			 * cell = new HBaseCellBean();
			 * cell.setCellEntity(Bytes.toBytes(rowkey), CF_NAME,
			 * CATEGORY_COLUMN_NAME, Bytes.toBytes("nihao:"+rowkey));
			 * cellArray.add(cell);
			 * 
			 * hbclient.addRecords(tablename,cellArray);
			 */

			/*
			 * byte[] CF_NAME = Bytes.toBytes("AppInfo"); byte[]
			 * CATEGORY_COLUMN_NAME = Bytes.toBytes("app_category");//app的类别
			 * String timestampstart = "2016061410"; String timestampend =
			 * "2016061411";
			 * 
			 * ResultScanner s =
			 * hbclient.getResultScanner(tablename,timestampstart,
			 * timestampend);
			 * 
			 * for(Result rs : s ){
			 * System.out.println(Bytes.toString(rs.getValue(CF_NAME,
			 * CATEGORY_COLUMN_NAME))); }
			 */

			/*
			 * //用户app标签 ArrayList<byte[]> list = new ArrayList<byte[]>();
			 * list.add(Bytes.toBytes("a.akakao.christmas.snowman"));
			 * list.add(Bytes.toBytes("a.akakao.adamrb"));
			 * list.add(Bytes.toBytes("a.akakao.danji_swimming")); byte[]
			 * CATEGORY_COLUMN_NAME = Bytes.toBytes("app_category");//app的类别
			 * Result[] results = hbclient.getRowsRecordByColumn("appInfo",
			 * list, CF_NAME, APP_LABEL); Result[] results =
			 * hbclient.getRowsRecord("appInfo", list); byte[] cellByte = null;
			 * for(Result result : results){
			 * 
			 * if(result == null) continue;
			 * 
			 * cellByte = result.getValue(CF_NAME, CATEGORY_COLUMN_NAME);
			 * 
			 * if(cellByte != null)
			 * System.out.println(Bytes.toString(cellByte)); }
			 */
			// System.out.println(hbclient.getColumns("appInfo_blackList", list,
			// Bytes.toBytes("AppInfo")));
			// hbclient.createTable("userInfo",familys );
			// hbclient.createTable("tablename", familys);
			// hbclient.closeConnection();
			// hbclient.deleteTable("test");
			// HbaseUtil.createTable(tablename, familys);
			// hbclient.createTable("appInfo_blackList",familys);
			// add record zkb
			// ArrayList<HBaseCellEntity> cellArray = new
			// ArrayList<HBaseCellEntity>();
			// HBaseCellEntity cell = new HBaseCellEntity();
			// cell.setRowKey(Bytes.toBytes("zkb"));
			// cell.setFamily(Bytes.toBytes("grade"));
			// cell.setValue(Bytes.toBytes("10"));
			// cell.setQualifier(Bytes.toBytes("sum"));
			// cellArray.add(cell);
			//
			// HBaseCellEntity cell1 = new HBaseCellEntity();
			// cell1.setRowKey(Bytes.toBytes("zkb"));
			// cell1.setFamily(Bytes.toBytes("course"));
			// cell1.setQualifier(Bytes.toBytes("ch"));
			// cell1.setValue(Bytes.toBytes("10"));
			// cellArray.add(cell1);

			// HbaseUtil.addRecords("ada", cellArray);
			// long currentTime = System.currentTimeMillis();
			// Result rs = getRowRecord (tablename, Bytes.toBytes("zkb"));
			// System.out.println(rs);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
