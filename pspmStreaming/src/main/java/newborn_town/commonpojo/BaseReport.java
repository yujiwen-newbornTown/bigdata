package newborn_town.commonpojo;

/**
 * @author chenhao
 * @version 创建时间：2016年9月23日 上午10:28:49
 */
public class BaseReport {
	
	private String id;
	private String day;
	private int publisher_id;
	private String publisher_slot;
	private int advertiser_id;
	private int gross_clicks;
	private int unique_clicks;
	private int conversions;
	private double revenue;
	private double profit;
	
	
	
	public String getId() {
		return id;
	}
	public void setId(String id) {
		this.id = id;
	}
	public String getDay() {
		return day;
	}
	public void setDay(String day) {
		this.day = day;
	}
	public int getPublisher_id() {
		return publisher_id;
	}
	public void setPublisher_id(int publisher_id) {
		this.publisher_id = publisher_id;
	}
	public String getPublisher_slot() {
		return publisher_slot;
	}
	public void setPublisher_slot(String publisher_slot) {
		this.publisher_slot = publisher_slot;
	}
	public int getAdvertiser_id() {
		return advertiser_id;
	}
	public void setAdvertiser_id(int advertiser_id) {
		this.advertiser_id = advertiser_id;
	}
	public int getGross_clicks() {
		return gross_clicks;
	}
	public void setGross_clicks(int gross_clicks) {
		this.gross_clicks = gross_clicks;
	}
	public int getUnique_clicks() {
		return unique_clicks;
	}
	public void setUnique_clicks(int unique_clicks) {
		this.unique_clicks = unique_clicks;
	}
	public int getConversions() {
		return conversions;
	}
	public void setConversions(int conversions) {
		this.conversions = conversions;
	}
	public double getRevenue() {
		return revenue;
	}
	public void setRevenue(double revenue) {
		this.revenue = revenue;
	}
	public double getProfit() {
		return profit;
	}
	public void setProfit(double profit) {
		this.profit = profit;
	}
	
	
	
	
	
}
