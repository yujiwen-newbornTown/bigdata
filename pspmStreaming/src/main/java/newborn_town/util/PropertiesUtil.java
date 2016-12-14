package newborn_town.util; 

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.apache.log4j.Logger;


/** 
 * @author chenhao
 * @version 创建时间：2016年7月29日 下午7:06:26 
 */
public class PropertiesUtil {
	
	static Logger logger=Logger.getLogger(PropertiesUtil.class); 
	
	/**
	 * 通过path路径获取propery文件
	 * @param path
	 * @return
	 */
	public static Properties getProperties(String path) {
		InputStream in = null;
		Properties prop = new Properties();
		try {
			in = PropertiesUtil.class.getResourceAsStream(path);
			prop.load(in);
		} catch (IOException e) {
			logger.error("PropertiesUtil getProperties Exception :" + e);
		} finally {
			try {
				in.close();
			} catch (IOException e) {
				logger.error("PropertiesUtil getProperties Exception :" + e);
			}
		}
		return prop;
	}

	/**
	 * 获取path路径下的propery文件中key的值
	 * @param key  
	 * @param path
	 * @return
	 */
	public static String getValue(String key,String path){
		return getProperties(path).getProperty(key);
	}
}
