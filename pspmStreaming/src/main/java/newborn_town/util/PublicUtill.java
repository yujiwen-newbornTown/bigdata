package newborn_town.util;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.math.BigDecimal;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hbase.util.Bytes;

/**
 * 公用工具类
 * 
 * @author yangyang
 *
 */
public class PublicUtill {

	/**
	 * StringList集合转换为byte数组集合
	 * 
	 * @param arlist
	 * @return
	 */
	public static ArrayList<byte[]> listToBytes(ArrayList<String> arlist) {

		ArrayList<byte[]> bytes = new ArrayList<byte[]>();

		for (String str : arlist) {
			bytes.add(Bytes.toBytes(str));
		}

		return bytes;
	}

	/**
	 * StringSet集合转换为byte数组集合
	 * 
	 * @param arlist
	 * @return
	 */
	public static ArrayList<byte[]> listToBytes(Set<String> arlist) {

		ArrayList<byte[]> bytes = new ArrayList<byte[]>();

		for (String str : arlist) {
			bytes.add(Bytes.toBytes(str));
		}

		return bytes;
	}

	/**
	 * HashMap<String, HashMap<String, Object>>集合转换成byte数据集合
	 * 
	 * @param map
	 * @return
	 */
	public static HashMap<byte[], HashMap<byte[], byte[]>> mapToBytes(
			HashMap<String, HashMap<String, Long>> maps) {

		if (null != maps && maps.size() > 0) {

			byte[] key = null;

			HashMap<byte[], HashMap<byte[], byte[]>> outMap = new HashMap<byte[], HashMap<byte[], byte[]>>();
			HashMap<String, Long> inMaps = null;

			for (Map.Entry<String, HashMap<String, Long>> map : maps.entrySet()) {

				key = Bytes.toBytes(map.getKey());

				inMaps = map.getValue();

				HashMap<byte[], byte[]> outInMap = new HashMap<byte[], byte[]>();

				for (Entry<String, Long> inMap : inMaps.entrySet()) {

					outInMap.put(Bytes.toBytes(inMap.getKey()),
							Bytes.toBytes(inMap.getValue()));
				}
				outMap.put(key, outInMap);
			}
			return outMap;
		}
		return null;
	}

	/**
	 * HashMap<String, HashMap<String, Long>>集合转换成byte数据集合HashMap<byte[],
	 * HashMap<byte[], Long>>
	 */
	public static HashMap<byte[], HashMap<byte[], Long>> mapToBytesLong(
			HashMap<String, HashMap<String, Long>> maps) {

		if (null != maps && maps.size() > 0) {

			byte[] key = null;

			HashMap<byte[], HashMap<byte[], Long>> outMap = new HashMap<byte[], HashMap<byte[], Long>>();
			HashMap<String, Long> inMaps = null;

			for (Map.Entry<String, HashMap<String, Long>> map : maps.entrySet()) {

				key = Bytes.toBytes(map.getKey());

				inMaps = map.getValue();

				HashMap<byte[], Long> outInMap = new HashMap<byte[], Long>();

				for (Entry<String, Long> inMap : inMaps.entrySet()) {

					outInMap.put(Bytes.toBytes(inMap.getKey()),
							inMap.getValue());
				}
				outMap.put(key, outInMap);
			}
			return outMap;
		}
		return null;
	}

	/**
	 * HashMap<String, HashMap<String, String>>集合转换成byte数据集合
	 * 
	 * @param map
	 * @return
	 */
	public static HashMap<byte[], HashMap<byte[], byte[]>> mapStringToBytes(
			HashMap<String, HashMap<String, String>> maps) {

		if (null != maps && maps.size() > 0) {

			byte[] key = null;

			HashMap<byte[], HashMap<byte[], byte[]>> outMap = new HashMap<byte[], HashMap<byte[], byte[]>>();
			HashMap<String, String> inMaps = null;

			for (Map.Entry<String, HashMap<String, String>> map : maps
					.entrySet()) {

				key = Bytes.toBytes(map.getKey());

				HashMap<byte[], byte[]> outInMap = new HashMap<byte[], byte[]>();

				inMaps = map.getValue();

				for (Entry<String, String> inMap : inMaps.entrySet()) {
					outInMap.put(Bytes.toBytes(inMap.getKey()),
							Bytes.toBytes(inMap.getValue()));
				}
				outMap.put(key, outInMap);
			}
			return outMap;
		}
		return null;
	}

	/*
	 * map<String,String> transfer map<byte[],byte[]>
	 */
	public static HashMap<byte[], byte[]> simpleMapToBytes(
			HashMap<String, String> maps) {

		if (null != maps && maps.size() > 0) {

			HashMap<byte[], byte[]> outMap = new HashMap<byte[], byte[]>();

			for (Map.Entry<String, String> map : maps.entrySet()) {
				outMap.put(Bytes.toBytes(map.getKey()),
						Bytes.toBytes(map.getValue()));
			}
			return outMap;
		}
		return null;
	}

	/**
	 * 在字符串指定位置添加字符串
	 * 
	 * @param src
	 *            需要添加的字符串
	 * @param dec
	 *            添加的内容
	 * @param position
	 *            字符串的下角标
	 * @return
	 */
	public static String insertString(String src, String dec, int position) {
		StringBuffer stringBuffer = new StringBuffer(src);
		return stringBuffer.insert(position, dec).toString();
	}

	/**
	 * 将内容写入文件
	 * 
	 * @param strings
	 *            内容
	 * @param path
	 *            路径
	 */
	public static void writeText(String strings, String path) {
		File file = new File(path);
		FileWriter fw = null;
		BufferedWriter writer = null;
		try {
			fw = new FileWriter(file, true);
			writer = new BufferedWriter(fw);
			writer.write(strings);
			writer.flush();
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			try {
				writer.close();
				fw.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}

	/**
	 * 判断2个数字大小
	 * 
	 * @param val1
	 *            数字一
	 * @param val2
	 *            数字二
	 * @return -1 第一个小于第二个 0 两个数相等 1 第一个大于第二个
	 */
	public static int compare(BigDecimal val1, BigDecimal val2) {
		if (val1.compareTo(val2) < 0) {
			return -1;
		}
		if (val1.compareTo(val2) == 0) {
			return 0;
		}
		if (val1.compareTo(val2) > 0) {
			return 1;
		}
		return 0;
	}

	public static HashMap<String, HashMap<String, String>> addFlag(
			HashMap<String, HashMap<String, String>> hash, String flag) {

		HashMap<String, String> newmap = new HashMap<String, String>();
		HashMap<String, HashMap<String, String>> newHash = new HashMap<String, HashMap<String, String>>();

		for (Entry<String, HashMap<String, String>> mapStr : hash.entrySet()) {

			HashMap<String, String> newmaps = mapStr.getValue();
			newmap = new HashMap<String, String>();
			for (Entry<String, String> maps : newmaps.entrySet()) {
				newmap.put(flag + maps.getKey(), maps.getValue());
			}

			newHash.put(mapStr.getKey(), newmap);
		}

		return newHash;
	}

	/**
	 * 将字符串MD5加密
	 * 
	 * @param s
	 * @return
	 */
	public static String MD5(String inStr) {
		
		MessageDigest md5 = null;
		try {
			md5 = MessageDigest.getInstance("MD5");
		} catch (Exception e) {
			e.printStackTrace();
			return "";
		}
		char[] charArray = inStr.toCharArray();
		byte[] byteArray = new byte[charArray.length];

		for (int i = 0; i < charArray.length; i++)
			byteArray[i] = (byte) charArray[i];

		byte[] md5Bytes = md5.digest(byteArray);

		StringBuffer hexValue = new StringBuffer();

		for (int i = 0; i < md5Bytes.length; i++) {
			int val = ((int) md5Bytes[i]) & 0xff;
			if (val < 16)
				hexValue.append("0");
			hexValue.append(Integer.toHexString(val));
		}

		return hexValue.toString();
	}

	/**
	 * 可逆的加密算法
	 * 
	 * @param inStr
	 * @return
	 */
	public static String KL(String inStr) {
		char[] a = inStr.toCharArray();
		for (int i = 0; i < a.length; i++) {
			a[i] = (char) (a[i] ^ 't');
		}
		String s = new String(a);
		return s;
	}

	/**
	 * 加密后解密
	 * 
	 * @param inStr
	 * @return
	 */
	public static String JM(String inStr) {
		char[] a = inStr.toCharArray();
		for (int i = 0; i < a.length; i++) {
			a[i] = (char) (a[i] ^ 't');
		}
		String k = new String(a);
		return k;
	}

	public static void main(String[] args) {
			String s ="1";
		  System.out.println("原始：" + s);   
		  System.out.println("MD5后：" + MD5(s));   
		  System.out.println("MD5后再加密：" + KL(MD5(s)));   
		  System.out.println("解密为MD5后的：" + JM(KL(MD5(s))));   
	}
}
