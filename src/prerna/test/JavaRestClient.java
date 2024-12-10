package prerna.test;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.codec.binary.Base64;
import org.apache.http.Header;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CookieStore;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.BasicCookieStore;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.JSONObject;

public class JavaRestClient {
	
	CookieStore cookieStore = new BasicCookieStore();
	String baseUrl = null;
	String insightId;

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		JavaRestClient jrc = new JavaRestClient();
		jrc.login("https://workshop.cfg.deloitte.com/cfg-ai-demo/Monolith/api", "488c161f-ba17-45ba-97a1-22915fb17f15", "43f214d4-c0fa-4587-9d41-438463d77a6d");
		//jrc.login("https://workshop.cfg.deloitte.com/cfg-ai-demo/Monolith/api", "abcd", "xyz");
		String py = "Py('<encode> 2 + 2</encode>')";
		String llm = "LLM(engine='4801422a-5c62-421e-a00c-05c6a9e15de8', command='hello')";
		jrc.runPixel(llm);
		//jrc.makeNewInsight();
		
	}
	
	public void login(String url, String access, String secret)
	{
		try {
			this.baseUrl = url;
			url = this.baseUrl +"/auth/whoami";
			String combined = access + ":" + secret;

			byte[] encodedBytes = Base64.encodeBase64(combined.getBytes());
			String stringEnc = Base64.encodeBase64String(combined.getBytes());
			System.err.println("Encoded " + stringEnc);
			System.err.println("decoded " + Base64.decodeBase64(stringEnc));
			
			HttpGet request = new HttpGet(url);
			request.addHeader("Authorization", "Basic " + stringEnc);
			//request.addHeader("disableRedirect", "true");
			CloseableHttpClient httpClient = HttpClientBuilder.create()
			        .setDefaultCookieStore(cookieStore)
			        .build();
			CloseableHttpResponse response = httpClient.execute(request);
			
			System.out.println(">>" + response.getStatusLine().toString());

			HttpEntity entity = response.getEntity();
			Header headers = entity.getContentType();
			System.out.println(headers);
			String result = EntityUtils.toString(entity);
            System.out.println(result);	
            System.err.println(cookieStore.getCookies());
            
            for (int cookieIndex = 0;cookieIndex < cookieStore.getCookies().size();cookieIndex++)
            	System.err.println(cookieStore.getCookies().get(cookieIndex).getName());
            
		} catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
	}
	
	
	public String runPixel(String pixel)
	{
		try {
			makeNewInsight();
			
			String url = this.baseUrl + "/engine/runPixel";
			HttpPost hp = new HttpPost(url);
			
			List<NameValuePair> urlParameters = new ArrayList<>();
			urlParameters.add(new BasicNameValuePair("insightId", this.insightId));
			urlParameters.add(new BasicNameValuePair("expression", pixel));

			hp.setEntity(new UrlEncodedFormEntity(urlParameters));
			CloseableHttpResponse response = getHttpClient().execute(hp);
			
			String jsonResponse = EntityUtils.toString(response.getEntity());

			JSONObject jo = new org.json.JSONObject(jsonResponse);

			System.err.println(jsonResponse);
			
			return jsonResponse;

		} catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;
	}
	
	public void makeNewInsight()
	{
		if(this.insightId == null)
		{
			try {
				
				String url = this.baseUrl + "/engine/runPixel";
				HttpPost hp = new HttpPost(url);
				
				List<NameValuePair> urlParameters = new ArrayList<>();
				urlParameters.add(new BasicNameValuePair("expression", "META | true"));
				urlParameters.add(new BasicNameValuePair("insightId", "new"));
	
				hp.setEntity(new UrlEncodedFormEntity(urlParameters));
				CloseableHttpResponse response = getHttpClient().execute(hp);
				
				String jsonResponse = EntityUtils.toString(response.getEntity());
	
				JSONObject jo = new org.json.JSONObject(jsonResponse);
	
				this.insightId = jo.getString("insightID");
				
				System.err.println(this.insightId);
				
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (ClientProtocolException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
	}
	
	private CloseableHttpClient getHttpClient()
	{
		CloseableHttpClient httpClient = HttpClientBuilder.create()
		        .setDefaultCookieStore(cookieStore)
		        .build();
		
		return httpClient;

	}
	

}
