package newborn_town.rawlogclean;

import java.text.ParseException;
import java.text.SimpleDateFormat;

import org.apache.commons.lang.StringUtils;

public class FormatFilter {
	
	public static boolean GeoFilter(String geo){
		return geo.matches("[a-z]{2}");
	}
	
	public static boolean PlacementFilter(String placement){
		return placement.matches("[0-9]+_[0-9]+");
	}

	public static boolean EmptyFilter(String packgeName) {
		return StringUtils.isEmpty(packgeName);
	}
	
	public static boolean CompanyFilter(String company, String[] validCompanyArray){
		for(String vs: validCompanyArray){
			if(vs.equals(company))
				return true;
		}
		return false;
	}
	
	public static boolean StringRegexFilter(String str,String regex){
		return str.matches(regex);
	}

	//packagename过滤函数
	public static boolean PackagenameFilter(String pkg){
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
	
	public static boolean lengthFilter(String str,int length){
		return str.length()==length;
	}
	
    public static boolean timeDifferenceFilter(String starttime, String endtime,long filtertime, SimpleDateFormat timeDifferenceDF){	
		long diff;
		try {
			
			if(StringUtils.isEmpty(starttime) && StringUtils.isEmpty(endtime)){
				return false;
			}
			
			diff = timeDifferenceDF.parse(endtime).getTime() - timeDifferenceDF.parse(starttime).getTime();
			
			return (diff <= filtertime);
		} catch (ParseException e) {
			return false;
		}
	}
}
