package newborn_town.util;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hbase.util.Bytes;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.log4j.Logger;

public class HttpUtil {
	static Logger logger=Logger.getLogger(HttpUtil.class); 

	/*static RequestConfig requestConfig = RequestConfig.custom()
			.setSocketTimeout(300000)
			.setConnectTimeout(300000)
			.setConnectionRequestTimeout(300000)
			.build();*/
	/**
	 * httpGetJson: 通过http get返回body字符串 
	 * @param String getUrl
	 * @return JSONObject
	 */

	public static String httpGet(String getUrl,int timeout){

		RequestConfig requestConfig = RequestConfig.custom()
				.setSocketTimeout(timeout)
				.setConnectTimeout(timeout)
				.setConnectionRequestTimeout(timeout)
				.build();
		CloseableHttpClient httpclient = HttpClients.createDefault();
		HttpGet getMethod = new HttpGet(getUrl);
		getMethod.setConfig(requestConfig);
		getMethod.setHeader("Accept-Encoding","gzip, deflate, sdch");
		getMethod.setHeader("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8");

		String getbodyStr = "";
		CloseableHttpResponse res = null;
		try {
			res  = httpclient.execute(getMethod);
			if(res.getStatusLine().getStatusCode() == 200) {
				getbodyStr = EntityUtils.toString(res.getEntity());
				//	System.out.println(getbodyStr);
			}
			else {
				logger.error("httpGetJson  Response Code: " + getUrl + "  " + res.getStatusLine().getStatusCode() );
				return null;
			}
		}catch(Exception e){
			logger.error(e.getMessage());
			return null;
		}finally{
			if(res != null){
				try {
					res.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return null;
				}
			}
			if (httpclient != null){
				try {
					httpclient.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return null;
				}
			}
		}
		return getbodyStr;
	}

	public static String httpPost(String postUrl, List<NameValuePair> namepairList,int timeout){
		HttpPost postMethod = new HttpPost(postUrl);
		CloseableHttpClient httpclient = HttpClients.createDefault();
		RequestConfig requestConfig = RequestConfig.custom()
				.setSocketTimeout(timeout)
				.setConnectTimeout(timeout)
				.setConnectionRequestTimeout(timeout)
				.build();

		postMethod.setConfig(requestConfig);
		HttpEntity requestenEntity;
		if(namepairList != null){
			try {
				requestenEntity = new UrlEncodedFormEntity(namepairList);
				postMethod.setEntity(requestenEntity);
			} catch (UnsupportedEncodingException e1) {
				// TODO Auto-generated catch block
				e1.printStackTrace();
				return null;
			}
		}

		String postbodyStr = "";
		CloseableHttpResponse res = null;
		try {
			res = httpclient.execute(postMethod);
			if(res.getStatusLine().getStatusCode() == 200) {
				postbodyStr = EntityUtils.toString(res.getEntity());
			}
			else {
				logger.error("httpPost Response Code: " + postUrl + "  " + res.getStatusLine().getStatusCode());
				System.out.println("httpPost Response Code: " + postUrl + "  " + res.getStatusLine().getStatusCode());
				return null;
			}
		}catch(Exception e){
			logger.error("httpPost " + e.getMessage());
			return null;
		}finally{
			if(res != null){
				try {
					res.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return null;
				}
			}
			if (httpclient != null){
				try {
					httpclient.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					return null;
				}
			}
		}

		return postbodyStr;
	}

	static public void main(String[] args){
		//System.out.println(httpGet("http://pspm.pingstart.com/api/campaigns?token=aac3343e-a60c-430a-882d-e135cd3f1145&publisher_id=1018&platform=Android&from=online"));
		String click = "ea06d330-6d83-11e6-b116-f23c91e25f22|2016-08-29 01:00:01|6528|ph|1|Shopping|com.shopee.ph|77||3|74|46|1|45|32|||||||com.android.launcher3 1.0 phone MyPhone MyPhone_MY28S android 5.1 en US 800_480 long high 480 800||112.198.101.69|2";
		String postback = "ea06d330-6d83-11e6-b116-f23c91e25f22|2016-12-10 02:17:54|52.90.15.34|||||||||||||||||mundo";
		List<NameValuePair> namepairList = new ArrayList<NameValuePair>();
		String postUrl = "http://pspm.pingstart.com/api/v4/postback_callback";
		namepairList.add(new BasicNameValuePair("click_info",click));
		namepairList.add(new BasicNameValuePair("postback_info",postback));
		HttpUtil.httpPost(postUrl, namepairList, 5000);
	}
}
