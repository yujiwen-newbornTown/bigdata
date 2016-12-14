package newborn_town.commonpojo; 
/** 
 * @author chenhao
 * @version 创建时间：2016年9月23日 下午2:04:27 
 */
public class ApiReport {
	
	private String id;
	private String day;
	private String publisher_slot;
	private int publisher_id;
	private String country;
	private int campaign_id;
	private int  gross_clicks;
	private int unique_clicks;
	private int conversions;
	private double revenue;
	private double profit;
	private double cost;
	
	public double getCost() {
		return cost;
	}
	public void setCost(double cost) {
		this.cost = cost;
	}
	public int getPublisher_id() {
		return publisher_id;
	}
	public void setPublisher_id(int publisher_id) {
		this.publisher_id = publisher_id;
	}
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
	public String getPublisher_slot() {
		return publisher_slot;
	}
	public void setPublisher_slot(String publisher_slot) {
		this.publisher_slot = publisher_slot;
	}
	public String getCountry() {
		return country;
	}
	public void setCountry(String country) {
		this.country = country;
	}
	
	public int getCampaign_id() {
		return campaign_id;
	}
	public void setCampaign_id(int campaign_id) {
		this.campaign_id = campaign_id;
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
