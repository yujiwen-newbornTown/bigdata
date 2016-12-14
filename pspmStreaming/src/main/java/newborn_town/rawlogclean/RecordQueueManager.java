package newborn_town.rawlogclean;


import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.Queue;

import org.apache.log4j.Logger;


public class RecordQueueManager {
	static Logger logger=Logger.getLogger(RecordQueueManager.class);

	private final static long INTERVAL_5MS = 5*60*1000;//5分钟的毫秒数
	
	public static boolean updateRecordsQueue(TimeRecordBean newRecord, Queue<TimeRecordBean> recordQueue, Map<String,String> recordMap,SimpleDateFormat timeDifferenceDF){
		
		//删除队列中大于 5分钟的记录
		while(!recordQueue.isEmpty() && !FormatFilter.timeDifferenceFilter(recordQueue.peek().getReal_time(), newRecord.getReal_time(),INTERVAL_5MS, timeDifferenceDF)){
			TimeRecordBean removeRecord = recordQueue.poll();
			recordMap.remove(removeRecord.getUnique_id());
		}

        if(recordMap.containsKey(newRecord.getUnique_id())){
        	return false;
        }
        
        if(recordQueue.offer(newRecord)){
        	recordMap.put(newRecord.getUnique_id(), newRecord.getReal_time());
		}else{
			logger.error("insert into queue failure: id: "+newRecord.getUnique_id()+" time: "+newRecord.getReal_time());
		}
        
        return true;
	}
	
}
