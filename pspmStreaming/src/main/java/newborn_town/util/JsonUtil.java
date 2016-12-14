package newborn_town.util;

import net.sf.json.JSONObject;

/**
 * json工具类
 * 
 * @author yangyang
 *
 */
public class JsonUtil {

	/**
	 * 对象转换json
	 * @param obj
	 * @return
	 */
	public static String Object2Json(Object obj) {
		
		JSONObject json = JSONObject.fromObject(obj);// 将java对象转换为json对象
		
		String str = json.toString();// 将json对象转换为字符串

		return str;
	}
}
