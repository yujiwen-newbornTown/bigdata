package newborn_town.util;

import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;



public class DateTransformUtil {
	static Logger logger=Logger.getLogger(DateTransformUtil.class); 
   
	public static String changeDateFormat(String initDateStr, SimpleDateFormat initformat, SimpleDateFormat outputformat){
		Date date = null;
		try{
			date = initformat.parse(initDateStr);
			return outputformat.format(date);
		}catch (ParseException e) {
			logger.error("getDateStrBeforeTargetDate exception: " + e.getMessage());
			return "";
		}
	}
	
	public static long DateToTimestamp(String timeString, SimpleDateFormat format){
		long time =0;
		try {
			 time = format.parse(timeString).getTime();
			return time;
		} catch (ParseException e) {
			return time;
		}
	}
	
	/**
	 * 返回当前时间(例： "2016:07:06 12:05:05")
	 * @return
	 */
	public static String currentTimeToString(){
		Date now = new Date(); 
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy:MM:dd HH:mm:ss");//可以方便地修改日期格式
		String date = dateFormat.format( now ); 
		return date;
	}
	
	/**
	 * 返回当前时间(例： "2016:07:06")
	 * @return
	 */
	public static String currentDayToString(){
		Date now = new Date(); 
		SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy:MM:dd");//可以方便地修改日期格式
		String date = dateFormat.format( now ); 
		return date;
	}
	
	public static String currentDayToStringFormat(String format){
		Date now = new Date(); 
		SimpleDateFormat dateFormat = new SimpleDateFormat(format);//可以方便地修改日期格式
		String date = dateFormat.format( now ); 
		return date;
	}
	
	/**
	 * 把时间转化为指定格式字符串
	 * @param date
	 * @param format
	 * @return
	 */
	public static String TimeToString(Date date , String format){
		SimpleDateFormat dateFormat = new SimpleDateFormat(format);//可以方便地修改日期格式
		String result = dateFormat.format( date ); 
		return result;
	}
	
	/**
	 * 把字符串格式的时间转换为date类型的时间
	 * @param time
	 * @return
	 */
	public static Date stringToDate(String time){
		
		SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
		
		Date date;
		
		try {
			date = sdf.parse(time);
			return date;
		} catch (ParseException e) {
			e.printStackTrace();
		}
		return null;
	}
	
	
	/**
	 * 获取某一天的前一天并转化为字符串
	 * @param day 时间
	 * @param format 获取前一天的时间格式
	 * @return
	 */
	public static String getBeforeDay(Date day , String format){
		
		Date dBefore = new Date();
		
		Calendar calendar = Calendar.getInstance(); //得到日历
		calendar.setTime(day);//把时间赋给日历
		calendar.add(Calendar.DAY_OF_MONTH, -1);  //设置为前一天
		dBefore = calendar.getTime();   //得到前一天的时间
		
		SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMdd"); //设置时间格式
		String defaultStartDate = sdf.format(dBefore);    //格式化前一天
		return defaultStartDate;
	}
	
	public static void main(String[] args){
		System.out.println(compareDateTime("2016-12-13 10:03:09","2016-12-13 10:03:04"));
	}
	
	
	public static boolean compareDateTime(String day1,String day2){
		DateFormat df = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss");
        try {
            Date dt1 = df.parse(day1);
            
            if(StringUtils.isEmpty(day2)){
            	return true;
            }
            Date dt2 = df.parse(day2);
            if (dt1.getTime() >= dt2.getTime()) {//dt1 在dt2前
                return true;
            } else if (dt1.getTime() < dt2.getTime()) {//dt2 在dt1前
                return false;
            } else {
                return false;
            }
        } catch (Exception exception) {
            exception.printStackTrace();
        }
        return false;
	}
	
	
}
