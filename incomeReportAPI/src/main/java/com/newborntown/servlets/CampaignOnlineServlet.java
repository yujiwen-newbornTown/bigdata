package com.newborntown.servlets;

import java.io.IOException;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.newborntown.model.IncomeReportProvider;

public class CampaignOnlineServlet extends HttpServlet{

	private static final long serialVersionUID = 1897366781881102482L;

	static Logger logger = Logger.getLogger(CampaignOnlineServlet.class);
	
	IncomeReportProvider incomeReportProvider = new IncomeReportProvider();

	@Override
	protected void doGet(HttpServletRequest request,
			HttpServletResponse response) throws ServletException,
			IOException {
		response.setContentType("text/json");
		response.setStatus(HttpServletResponse.SC_OK);
		try{
			logger.info(request.getQueryString());
			
			String methodType = request.getParameter("methodtype");
			
			String startDate = request.getParameter("startdate");
			String endDate = request.getParameter("enddate");
			
			if(methodType.equals("payout")){
				response.getWriter().write(incomeReportProvider.getPayOutOfPublisher(startDate, endDate));
			}else
			    response.getWriter().write(incomeReportProvider.getRevenuePayOutProfitOfSource(startDate, endDate));
		}catch(Exception e){
			logger.error(e.getMessage());
		}
	}
	
}
