package com.newborntown.jobschedule;


import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Handler;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.handler.HandlerList;
import org.eclipse.jetty.server.nio.SelectChannelConnector;
import org.eclipse.jetty.servlet.ServletHandler;
import org.eclipse.jetty.util.thread.ExecutorThreadPool;

import com.newborntown.dao.impl.PayOutDaoImpl;
import com.newborntown.service.GetUpstreamRevenue;
import com.newborntown.servlets.CampaignOnlineServlet;

public class IncomeReportApiJob {
	static Logger logger = Logger.getLogger(IncomeReportApiJob.class);
	
	
	public static void RunApiService(){
		Server server = new Server();
		server.setThreadPool(new ExecutorThreadPool(20, 40, 30000));
		
		//connector
		SelectChannelConnector connector = new SelectChannelConnector();
		connector.setPort(9111);
		server.addConnector(connector);
		 
        //online api handler
		ServletHandler onlineHandler = new ServletHandler();
		onlineHandler.addServletWithMapping(CampaignOnlineServlet.class, "/incomeReport");
		
		//handler list
		HandlerList handlerlist = new HandlerList();
		handlerlist.setHandlers(new Handler[]{onlineHandler});
		server.setHandler(handlerlist);
		
		try{
			server.start();
			server.join();
		}catch(Exception e){
			logger.error(e.getMessage());
		}
	}

	public static void main(String[] args){
		
		String index = args[0];
		
		switch(index){
		
		case "StartIncomeReportJob" :
			if(args.length != 1){
				logger.error("StartIncomeReportJob argsException");
				break;
			}else{
				RunApiService();
				break;	
			}
		
		case "InsertPayOutToMongoJob" :
			if(args.length != 2){
				logger.error("StartIncomeReportJob argsException");
				break;
			}else{
				new PayOutDaoImpl().startToPayOutMongoJob(args[1]);
				break;	
			}
		
		case "InsertRevenueToMongoJob" :
			if(args.length != 2){
				logger.error("StartIncomeReportJob argsException");
				break;
			}else{
				new GetUpstreamRevenue().InsertAllUpstreamRevenueToMongo(args[1]);
				break;	
			}
		}
	}
}
