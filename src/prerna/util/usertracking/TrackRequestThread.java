package prerna.util.usertracking;

import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;

import com.google.gson.Gson;

public class TrackRequestThread extends Thread {

	private static final Gson GSON = new Gson();
	
	private static String endpoint;
	
	public static void setEndpoint(String endpoint) {
		endpoint = endpoint.trim();
		if(!endpoint.endsWith("/")) {
			endpoint = endpoint + "/";
		}
		TrackRequestThread.endpoint = endpoint;
	}
	
	private String type;
	private List<Object[]> rows;
	
	public TrackRequestThread(String type, List<Object[]> rows) {
		this.type = type;
		this.rows = rows;
	}
	
	@Override
	public void run() {
		HttpClient httpclient = HttpClients.createDefault();
		HttpPost httppost = new HttpPost(TrackRequestThread.endpoint + "track/" + this.type);

		// request parameters and other properties.
		List<NameValuePair> params = new ArrayList<NameValuePair>(1);
		params.add(new BasicNameValuePair("values", GSON.toJson(rows)));
		try {
			httppost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
			httppost.setHeader("Content-Type", "application/x-www-form-urlencoded");
			// execute and get the response.
			httpclient.execute(httppost);
			
//			HttpResponse response = httpclient.execute(httppost);
//			HttpEntity entity = response.getEntity();
//			if (entity != null) {
//			    InputStream instream = entity.getContent();
//			    try {
//			        String responseString = IOUtils.toString(instream);
//			        System.out.println(responseString);
//			    } finally {
//			        instream.close();
//			    }
//			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
}
