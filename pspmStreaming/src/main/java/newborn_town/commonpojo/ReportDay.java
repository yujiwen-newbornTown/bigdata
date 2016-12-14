package newborn_town.commonpojo;

/**
 * pspm report data(天)
 * 
 * @author yangyang
 *
 */
public class ReportDay {
	private String _id = "";//mogondb索引
	private int advertiser_id;
	private int advertiser_type;
	private int publisher_id;
	private int campaign_id;
	private String geo = "";
	private int platform;
	private String category = "";
	private String packageName = "";
	private int quality_1;
	private int quality_2;
	private int quality_3;
	private int publisher_type;
	private String publisher_slot = "";
	private String sub_1 = "";
	private String sub_2 = "";
	private String sub_3 = "";
	private int am_id;
	private int bd_id;
	private int pm_id;
	private int impressions;
	private int gross_clicks = 0;
	private int unique_clicks = 0;
	private int conversions = 0;
	private double revenue = 0;  //weget
	private double cost = 0; //payout
	private double profit = 0; // weget - payout
	private String day = "";
	private String month = "";
	private String year = "";
	private String status="0";

	public String get_id() {
		return _id;
	}

	public void set_id(String _id) {
		this._id = _id;
	}

	public int getAdvertiser_id() {
		return advertiser_id;
	}

	public void setAdvertiser_id(int advertiser_id) {
		this.advertiser_id = advertiser_id;
	}

	public int getAdvertiser_type() {
		return advertiser_type;
	}

	public void setAdvertiser_type(int advertiser_type) {
		this.advertiser_type = advertiser_type;
	}

	public int getPublisher_id() {
		return publisher_id;
	}

	public void setPublisher_id(int publisher_id) {
		this.publisher_id = publisher_id;
	}

	public int getCampaign_id() {
		return campaign_id;
	}

	public void setCampaign_id(int campaign_id) {
		this.campaign_id = campaign_id;
	}

	public String getGeo() {
		return geo;
	}

	public void setGeo(String geo) {
		this.geo = geo;
	}

	public int getPlatform() {
		return platform;
	}

	public void setPlatform(int platform) {
		this.platform = platform;
	}

	public String getCategory() {
		return category;
	}

	public void setCategory(String category) {
		this.category = category;
	}

	public String getPackageName() {
		return packageName;
	}

	public void setPackageName(String packageName) {
		this.packageName = packageName;
	}

	public int getQuality_1() {
		return quality_1;
	}

	public void setQuality_1(int quality_1) {
		this.quality_1 = quality_1;
	}

	public int getQuality_2() {
		return quality_2;
	}

	public void setQuality_2(int quality_2) {
		this.quality_2 = quality_2;
	}

	public int getQuality_3() {
		return quality_3;
	}

	public void setQuality_3(int quality_3) {
		this.quality_3 = quality_3;
	}

	public int getPublisher_type() {
		return publisher_type;
	}

	public void setPublisher_type(int publisher_type) {
		this.publisher_type = publisher_type;
	}

	public String getPublisher_slot() {
		return publisher_slot;
	}

	public void setPublisher_slot(String publisher_slot) {
		this.publisher_slot = publisher_slot;
	}

	public String getSub_1() {
		return sub_1;
	}

	public void setSub_1(String sub_1) {
		this.sub_1 = sub_1;
	}

	public String getSub_2() {
		return sub_2;
	}

	public void setSub_2(String sub_2) {
		this.sub_2 = sub_2;
	}

	public String getSub_3() {
		return sub_3;
	}

	public void setSub_3(String sub_3) {
		this.sub_3 = sub_3;
	}

	public int getAm_id() {
		return am_id;
	}

	public void setAm_id(int am_id) {
		this.am_id = am_id;
	}

	public int getBd_id() {
		return bd_id;
	}

	public void setBd_id(int bd_id) {
		this.bd_id = bd_id;
	}

	public int getPm_id() {
		return pm_id;
	}

	public void setPm_id(int pm_id) {
		this.pm_id = pm_id;
	}

	public int getImpressions() {
		return impressions;
	}

	public void setImpressions(int impressions) {
		this.impressions = impressions;
	}

	public int getConversions() {
		return conversions;
	}

	public void setConversions(int conversions) {
		this.conversions = conversions;
	}


	public void setProfit(float profit) {
		this.profit = profit;
	}

	public String getDay() {
		return day;
	}

	public void setDay(String day) {
		this.day = day;
	}

	public String getMonth() {
		return month;
	}

	public void setMonth(String month) {
		this.month = month;
	}

	public String getYear() {
		return year;
	}

	public void setYear(String year) {
		this.year = year;
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

	public double getRevenue() {
		return revenue;
	}

	public void setRevenue(double revenue) {
		this.revenue = revenue;
	}

	public double getCost() {
		return cost;
	}

	public void setCost(double cost) {
		this.cost = cost;
	}

	public double getProfit() {
		return profit;
	}

	public void setProfit(double profit) {
		this.profit = profit;
	}

	public String getStatus() {
		return status;
	}

	public void setStatus(String status) {
		this.status = status;
	}
}
