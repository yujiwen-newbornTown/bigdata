package newborn_town.rawlogclean;


public class TimeRecordBean {
	private String unique_id;
	private String real_time;
	
	public TimeRecordBean(String unique_id, String real_time){
		this.unique_id = unique_id;
		this.real_time = real_time;
	}

	public String getUnique_id() {
		return unique_id;
	}

	public void setUnique_id(String unique_id) {
		this.unique_id = unique_id;
	}

	public String getReal_time() {
		return real_time;
	}

	public void setReal_time(String real_time) {
		this.real_time = real_time;
	}

	

	@Override
	public String toString() {
		return "UniqueLog [unique_id=" + unique_id + ", real_time=" + real_time
				+ "]";
	}
}
