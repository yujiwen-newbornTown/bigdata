package newborn_town.rawlogclean;

import newborn_town.util.MongoUtil;

/**
 * @author  yujiwen E-mail: yujiwen@newborn-town.com
 * @version 创建时间：2016年11月16日 下午3:17:35
 * 
 */
public class PostbackStatistic {
	
	static MongoUtil mongoUtil = null;
	
	public static void main(String[] args){
		
		//db.campaignReport.aggregate([{$match:{"day":"2016-11-15"}},{"$group":{_id:"$day",count:{$sum:'$conversions'}}}])
		mongoUtil = new MongoUtil("pspm", "campaignReport");
//		mongoUtil
		
	}
	
}
