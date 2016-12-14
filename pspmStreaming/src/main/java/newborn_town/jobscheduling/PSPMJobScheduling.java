package newborn_town.jobscheduling;

import newborn_town.pspm.online.topology.PSPMClickLogTopology;
import newborn_town.pspm.online.topology.PSPMConversionLogToHBaseTopology;
import newborn_town.pspm.online.topology.PSPMConversionLogTopology;

import org.apache.log4j.Logger;

/**
 * pspm系统实时任务调度
 * 
 * 
 * @author yangyang
 *
 */
public class PSPMJobScheduling {
	
	static Logger logger=Logger.getLogger(PSPMJobScheduling.class); 
	
	public static void main(String[] args) {
		
		String index = args[0];
		
		switch(index){
		
		case "PSPMClickLogToMongoAndHBase" :
			if(args.length != 2){
				logger.error("InsterUserInfoJob argsException");
				break;
			}else{
				PSPMClickLogTopology.run(args[1]);
				break;	
			}
		case "PSPMConversionLogToMongo" :
			if(args.length != 2){
				logger.error("UpdateAppInfoJob argsException");
				break;
			}else{
				PSPMConversionLogTopology.run(args[1]);
				break;
			}
		case "PSPMConversionLogToHBase" :
			if(args.length != 2){
				logger.error("UpdateAppInfoJob argsException");
				break;
			}else{
				PSPMConversionLogToHBaseTopology.run(args[1]);
				break;
			}
		default :
			break;
		}
	}
}
