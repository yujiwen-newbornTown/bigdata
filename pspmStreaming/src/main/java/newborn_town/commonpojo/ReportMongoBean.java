package newborn_town.commonpojo;

/**
 * @author yujiwen E-mail: yujiwen@newborn-town.com
 * @version 创建时间：2016年11月24日 上午11:19:15
 * 
 */
public class ReportMongoBean {

	// public String _id;
	public String day;
	public String publisher_slot;
	public String geo;
	public String campaign_id;
	public String publisher_id;
	public String advertiser_id;
	public int gross_clicks;
	public int unique_clicks;

	// public String getId() {
	// return _id;
	// }
	// public void setId(String _id) {
	// this._id = _id;
	// }

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
		return geo;
	}

	public void setCountry(String geo) {
		this.geo = geo;
	}

	public String getCampaign_id() {
		return campaign_id;
	}

	public void setCampaign_id(String campaign_id) {
		this.campaign_id = campaign_id;
	}

	public String getAdvertiser_id() {
		return advertiser_id;
	}

	public void setAdvertiser_id(String advertiser_id) {
		this.advertiser_id = advertiser_id;
	}

	public String getPublisher_id() {
		return publisher_id;
	}

	public void setPublisher_id(String publisher_id) {
		this.publisher_id = publisher_id;
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

}
