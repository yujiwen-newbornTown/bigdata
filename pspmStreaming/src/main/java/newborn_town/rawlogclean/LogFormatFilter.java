package newborn_town.rawlogclean;


import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.commons.lang.StringUtils;

public class LogFormatFilter {
	static final int IMPRESSION_MIN_FIELD_NUM = 32;
	static final int CLICK_MIN_FIELD_NUM = 32;
	static final int INSTALL_MIN_FIELD_NUM =  22;
	static final int APPLIST_MIN_FIELD_NUM =  6;
	private final static String GEO_REGEX = "[a-z]{2}";
	private final static String[] VALID_COMPANY_ARRAY = {"PingStart", "CMS", "SoloRTB"};
	private final static long INTERVAL_3DS = 3*24*60*60*1000;//3天的毫秒数
	private final static int AID_MIN_LENGTH = 10;
	private static SimpleDateFormat timeDifferenceDF = new SimpleDateFormat("yyyy:MM:dd HH:mm:ss");

	static public String[] checkImpressionLogFormat(String impressionLog){
		impressionLog = impressionLog.trim();

		String[] stringArray = impressionLog.split("\\|");
		//原始数据字段信息不充分，过滤掉
		if(stringArray.length < IMPRESSION_MIN_FIELD_NUM){
			return null;
		}

		//real_time request_time 相差3天的过滤掉
		if(!timeDifferenceFilter(stringArray[16], stringArray[0], INTERVAL_3DS, timeDifferenceDF)){
			return null;
		}

		//placement格式   geo不是两位小写英文字母  pkg是纯数字或如22.33.33形式  source不是给定类别  aid不是16位  unique_id为空
		//以上条件满足一点即过滤掉
		if(!PlacementFilter(stringArray[3])||
				!StringRegexFilter(stringArray[4], GEO_REGEX)||
				!PackagenameFilter(stringArray[9])||
				!CompanyFilter(stringArray[13], VALID_COMPANY_ARRAY)||
				!lengthFilter(stringArray[22], AID_MIN_LENGTH)||
				EmptyFilter(stringArray[28])){
			return null;
		}

		return stringArray;
	}
	
	static public String[] checkClickLogFormat(String clickLog){
		clickLog = clickLog.trim();

		String[] stringArray = clickLog.split("\\|");
		//原始数据字段信息不充分，过滤掉
		if(stringArray.length < CLICK_MIN_FIELD_NUM){
			return null;
		}

		//real_time request_time 相差3天的过滤掉
		if(!timeDifferenceFilter(stringArray[16], stringArray[0], INTERVAL_3DS, timeDifferenceDF)){
			return null;
		}

		//placement为空   geo不是两位小写英文字母  pkg是纯数字或如22.33.33形式  source不是给定类别  aid不是16位  unique_id为空
		//以上条件满足一点即过滤掉
		if(!PlacementFilter(stringArray[3])||
				!StringRegexFilter(stringArray[4], GEO_REGEX)||
				!PackagenameFilter(stringArray[9])||
				!CompanyFilter(stringArray[13], VALID_COMPANY_ARRAY)||
				!lengthFilter(stringArray[22], AID_MIN_LENGTH)||
				EmptyFilter(stringArray[28])){
			return null;
		}

		return stringArray;
	}
	
	static public String[] checkInstallLogFormat(String installLog){
		installLog = installLog.trim();
		String [] stringArray = installLog.split("\\|");
		//原始数据字段信息不充分，过滤掉
		if(stringArray.length < INSTALL_MIN_FIELD_NUM){
			return null;
		}
		
		// source不是给定类别  aid不是16位
		//以上条件满足一点即过滤掉
		if(!CompanyFilter(stringArray[12], VALID_COMPANY_ARRAY)||
				!lengthFilter(stringArray[21], AID_MIN_LENGTH)){
			return null;
		}
		
		return stringArray;
	}
	
	static public String[] checkApplistLogFormat(String clickLog){
		clickLog = clickLog.trim();

		String [] stringArray = clickLog.split("\\|");
		//原始数据字段信息不充分，过滤掉
		if(stringArray.length < APPLIST_MIN_FIELD_NUM){
			return null;
		}

		//geo不是两位小写英文字母  aid不是16位
		//以上条件满足一点即过滤掉
		if(!StringRegexFilter(stringArray[4], GEO_REGEX)||
				!lengthFilter(stringArray[1], AID_MIN_LENGTH)){
			return null;
		}

		return stringArray;
	}
	
	
	private static boolean PlacementFilter(String placement){
		return placement.matches("[0-9]+_[0-9]+");
	}

	private static boolean EmptyFilter(String packgeName) {
		return StringUtils.isEmpty(packgeName);
	}
	
	private static boolean CompanyFilter(String company, String[] validCompanyArray){
		for(String vs: validCompanyArray){
			if(vs.equals(company))
				return true;
		}
		return false;
	}
	
	private static boolean StringRegexFilter(String str,String regex){
		return str.matches(regex);
	}

	//packagename过滤函数
	private static boolean PackagenameFilter(String pkg){
		String PKG_REGEX = "[0-9]+";
		if(FormatFilter.StringRegexFilter(pkg, PKG_REGEX)){
			return false;
		}

		String[] array = pkg.split("\\.");
		int counter = 0;
		for (int i = 0; i < array.length; i++) {
			if(FormatFilter.StringRegexFilter(array[i], PKG_REGEX)){
				counter++;
			}
		}
		if(counter==array.length){
			return false;
		}

		return true;
	}
	
	private static boolean lengthFilter(String str,int length){
		return str.length()>length;
	}
	
	private static boolean timeDifferenceFilter(String starttime, String endtime,long filtertime, SimpleDateFormat timeDifferenceDF){	
		long diff;
		try {
			diff = timeDifferenceDF.parse(endtime).getTime() - timeDifferenceDF.parse(starttime).getTime();
			return (diff <= filtertime);
		} catch (ParseException e) {
			return false;
		}
	}
}
