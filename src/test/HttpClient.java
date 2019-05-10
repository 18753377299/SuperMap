package test;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
	/**
 * @author  作者 E-mail: 
 * @date 创建时间：2018年11月29日 下午3:19:42
 * @version 1.0 
 * @parameter 
 * @since  
 * @return  */
public class HttpClient {
	private static final String USER_AGENT = "Mozilla/5.0";

    private static final String GET_URL = "http://10.10.1.156:8091/iserver/services/data-FXDT/rest/data/datasources/eqNew/datasets/eq_hazard_source_zone/gridValue.json?x=120.85419&y=29.81507";
    
	@SuppressWarnings("unused")
	private static void main(String [] args){
		try {
//			sendGET();
			CloseableHttpClient httpClient = HttpClients.createDefault();
	        HttpGet httpGet = new HttpGet(GET_URL);
	        httpGet.addHeader("User-Agent", USER_AGENT);
	        CloseableHttpResponse httpResponse = httpClient.execute(httpGet);

	        System.out.println("GET Response Status:: "
	                + httpResponse.getStatusLine().getStatusCode());

	        BufferedReader reader = new BufferedReader(new InputStreamReader(
	                httpResponse.getEntity().getContent()));

	        String inputLine;
	        StringBuffer response = new StringBuffer();

	        while ((inputLine = reader.readLine()) != null) {
	            response.append(inputLine);
	        }
	        reader.close();
	        // print result
	        System.out.println(response.toString());
	        httpClient.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
//	 private static void sendGET() throws IOException {
//	        CloseableHttpClient httpClient = HttpClients.createDefault();
//	        HttpGet httpGet = new HttpGet(GET_URL);
//	        httpGet.addHeader("User-Agent", USER_AGENT);
//	        CloseableHttpResponse httpResponse = httpClient.execute(httpGet);
//
//	        System.out.println("GET Response Status:: "
//	                + httpResponse.getStatusLine().getStatusCode());
//
//	        BufferedReader reader = new BufferedReader(new InputStreamReader(
//	                httpResponse.getEntity().getContent()));
//
//	        String inputLine;
//	        StringBuffer response = new StringBuffer();
//
//	        while ((inputLine = reader.readLine()) != null) {
//	            response.append(inputLine);
//	        }
//	        reader.close();
//	        // print result
//	        System.out.println(response.toString());
//	        httpClient.close();
//	    }
}
