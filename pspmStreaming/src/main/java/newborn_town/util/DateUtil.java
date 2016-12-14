package newborn_town.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.log4j.Logger;

/**
 * 时间工具类
 * @author Yangyang
 *
 */
public class DateUtil {
	
	static Logger logger=Logger.getLogger(DateUtil.class); 
	
	/**
	 * 获取下一天
	 * @param date
	 * @return
	 */
	public static String getNextDay(Date date) { 
        Calendar calendar = Calendar.getInstance();  
        calendar.setTime(date);  
        calendar.add(Calendar.DAY_OF_MONTH, +1);  
        date = calendar.getTime(); 
        SimpleDateFormat df=new SimpleDateFormat("yyyyMMdd"); 
        String nextDate =df.format(date);
        return nextDate;  
    }  
	
	/**
	 * 是否在"23:59:00"和 "23:59:59"区间之间
	 * @param date  时间
	 * @return
	 * @throws ParseException
	 */
	public static boolean isInDates(String date){
		
		// 格式化时间
		SimpleDateFormat df = new SimpleDateFormat("HH:mm:ss");
		
		try {
			// 格式化时间区间
			Date dateAfter = df.parse("23:59:59");
			Date dateBefor = df.parse("23:59:00");
			
			Date time = df.parse(date);

			if (time.before(dateAfter) && time.after(dateBefor)) {
				return true;
			} else {
				return false;
			}
		} catch (ParseException e) {
			logger.error("DateUtil isInDates Exception" + e);
		}
		return false;
	}
	
	/**
	 * string转date类型
	 * @param format 转换格式
	 * @return
	 */
	public static Date getStringToDate(String time,String format){
		
		SimpleDateFormat sdf = new SimpleDateFormat(format);
		
		try {
			Date date = sdf.parse(time);
			return date;
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	/**
	 * 判断两个时间相差多少
	 * @param endDate
	 * @param nowDate
	 * @param secondNum 秒数
	 * @return
	 */
	public static boolean getDatePoor(String startDate, String endDate,SimpleDateFormat format,int secondNum) {
		 
		Date start;
		Date end;
		
		try {
			start = format.parse(startDate);
			end = format.parse(endDate);
		} catch (ParseException e) {
			return false;
		}
		
	    return (end.getTime()-start.getTime()) > (1000 * secondNum);
	}
	
	
	/**
	 * 计算两时间相差秒数
	 * @param time1
	 * @param time2
	 * @return
	 */
	public static long getTimeGap(String time1,String time2){
		DateFormat df = new SimpleDateFormat("yyyy:MM:dd HH:mm:ss");
		long days = 0;
		try{
		    Date d1 = df.parse(time2);
		    Date d2 = df.parse(time1);
		    long diff = d1.getTime() - d2.getTime();
		    days = diff / 1000;
		}
		catch (Exception e){
			e.printStackTrace();
		}
		return days;
	}
	
	public static boolean getCrossDayStatus(String time1,String time2){
		time1 = time1.replaceAll(":", "").substring(0, 8);
		time2 = time2.replaceAll(":", "").substring(0, 8);
		if(time1.equals(time2)){
			return true;
		}else{
			return false;
		}
		
	}
	public static long getCrossTime(String time1,String time2){
		
		DateFormat df = new SimpleDateFormat("yyyy:MM:dd HH:mm:ss");
		long days = 0;
		try{
		    Date d1 = df.parse(time2);
		    Date d2 = df.parse(time1);
		    long diff = d1.getTime() - d2.getTime();
		    days = diff / (1000);
		}
		catch (Exception e){
			e.printStackTrace();
		}
		return days;
	}
	
	/**
	 * 获得时间字符串中的年
	 * @return
	 */
	public static String getYear(String dateStr){
		return dateStr.substring(0,4);
	}
	
	/**
	 * 获得时间字符串中的月
	 * @return
	 */
	public static String getMonth(String dateStr){
		return dateStr.substring(0,7);
	}
	
	/**
	 * 获得时间字符串中的日
	 * @return
	 */
	public static String getDay(String dateStr){
		return dateStr.substring(0,10);
	}
	
	/**
	 * 获得时间字符串中的小时
	 * @return
	 */
	public static String getHour(String dateStr){
		return getDay(dateStr) + " " + dateStr.substring(11,13);
	}
	
	public static void main(String[] args) {
		//System.out.println(getDatePoor("2016:07:03 00:00:01", "2016:07:03 00:01:00"));
		//Date date = new Date();
		//System.out.println(getCrossTime("2016:07:03 02:00:00","2016:07:03 02:15:46"));
		
		/*System.out.println(getDatePoor("2016-08-18 11:07:11","2016-08-18 11:09:11", new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss"),5));*/
		
		
		System.out.println(getHour("2016-08-18 11:07:11"));
	}
	
}
