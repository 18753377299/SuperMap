package test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.Charset;

import org.apache.commons.lang3.StringUtils;
	/**
 * @author  作者 E-mail: 
 * @date 创建时间：2018年11月23日 上午10:36:02
 * @version 1.0 
 * @parameter 
 * @since  
 * @return  */
public class ThreadTest {
	public static void main(String[]args){
		
		System.out.println("hello");
		
		MoreThreadTest moreThreadTest =new MoreThreadTest();
		String url ="http://10.10.1.156:8090/iserver/services/data-FXDT/rest/data/datasources/china/datasets/hail_hazard_scale_1km/gridValue.json?x=120.85419&y=29.81507";
		String  xmlString = moreThreadTest.connectServletUserXmlFile(url);
		System.out.println("输出的XML:"+xmlString);
		
//		for(int i =0 ;i<100;i++){
//			String url = "http://10.10.1.156:8090/iserver/services/data-FXDT/rest/data/datasources/china/datasets/thunderstorm_hazard_scale_1km/gridValue.json?x=119.940375&y=31.100143";
////			System.out.println("第"+i+"条数据");
//			MoreThreadTest  mThreadT = new MoreThreadTest(url,i);
//			mThreadT.start();
//		}		
		System.out.println("hello22");
	}
}
class MoreThreadTest extends Thread {
	
	private int start;
	private int end;
	private String url;
//	
//	public MoreThread(int start, int end) {
//		super();
//		this.start = start;
//		this.end = end;
//	}
	public MoreThreadTest(){
		super();
	}
	 public MoreThreadTest(String url,int start) {
			super();
			this.url = url;
			this.start = start;
	 }
	
	
	public  void run(){
//		this.updateDtvData(start,end);
		String  data = this.doGet(url);
		if(StringUtils.isNotBlank(data)){
			System.out.println("第" + start + "条数据==============="+data);
		}
	}	
	public static String doGet(String url) {
	        HttpClient client = new HttpClient();
	        GetMethod method = new GetMethod(url);
	        method.getParams().setParameter(HttpMethodParams.RETRY_HANDLER,new DefaultHttpMethodRetryHandler());
	        try {
	            client.executeMethod(method);
	            if (method.getStatusCode() == HttpStatus.SC_OK) {
	                return StreamUtils.copyToString(method.getResponseBodyAsStream(), Charset.forName("utf-8"));
	            }
	        } catch (IOException e) {
	        	e.printStackTrace();
	        	System.out.println("异常的URL:"+url);
//			        	LOGGER.info("执行HTTP Get请求" + url + "时，发生异常！",e);
//			        	throw new RuntimeException("执行HTTP Get请求" + url + "时，发生异常！",e);
	        } finally {
	            method.releaseConnection();
	        }
	        return "";
	 }
	
	public String connectServletUserXmlFile(String url) {
		String strReturnXML = "";
		HttpURLConnection httpConnection;
		try {
			// 1、打开连接
			httpConnection = (HttpURLConnection) new URL(url).openConnection();
//			httpConnection.setRequestMethod("POST");
			httpConnection.setRequestMethod("GET");
			httpConnection.setDoOutput(true);
			httpConnection.setDoInput(true);
			httpConnection.setAllowUserInteraction(true);
			httpConnection.connect();
			// 2、发送数据
			OutputStream outputStream = httpConnection.getOutputStream();
			outputStream.close();
			// 3、返回数据
			InputStreamReader inputStreamReader = new InputStreamReader(httpConnection.getInputStream(), "GBK");
			BufferedReader bufferedReader = new BufferedReader(inputStreamReader);
			String inputLine = "";
			StringBuffer inputLines = new StringBuffer();
			while ((inputLine = bufferedReader.readLine()) != null) {
				inputLines.append(inputLine);
			}
			inputStreamReader.close();
			bufferedReader.close();
			// 4、关闭连接
			httpConnection.disconnect();
			strReturnXML = inputLines.toString();

		} catch (MalformedURLException e) {
			e.printStackTrace();
			throw new RuntimeException("连接异常:" + e);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return strReturnXML;
	}
}