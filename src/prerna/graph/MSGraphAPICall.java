package prerna.graph;

import java.net.URLEncoder;

import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.util.SocialPropertiesUtil;

public class MSGraphAPICall {
	 
	private static final Logger classLogger = LogManager.getLogger(MSGraphAPICall.class);
	private static SocialPropertiesUtil socialData = null;

 
	static {
		socialData = SocialPropertiesUtil.getInstance();
	}
	/**
	 *This method use to get users data from graph api with next link having subsequent userdata
	 *@param accessToken, searchTerm, nextLink
	 *@return String
	 */
	public String getUserDetails(AccessToken accessToken, String searchTerm, String nextLink) throws Exception {
		classLogger.info("getUserDetails based on Graph Api");
		CloseableHttpClient httpClient = HttpClients.createDefault();
		String uri;
		if(nextLink == null) {
		 uri = URLEncoder.encode("\"displayName:" + searchTerm + "\" OR \"mail:" + searchTerm
				+ "\" OR \"userPrincipalName:" + searchTerm + "\"", java.nio.charset.StandardCharsets.UTF_8.toString());
		 uri = socialData.getProperty("ms_graph_url") + "/v1.0/users?$search=" + uri + "&$top=999&$count=true";
		}else {
			uri = nextLink;
		}
		HttpGet httpGet = new HttpGet(uri);
 
        // Set headers
		httpGet.setHeader("Authorization", "Bearer " + accessToken.getAccess_token());
		httpGet.setHeader("Accept", "application/json");
		httpGet.setHeader("ConsistencyLevel", "eventual");
 
        // Execute the request
		HttpResponse response = httpClient.execute(httpGet);
		String jsonResponse = EntityUtils.toString(response.getEntity());
 
		return jsonResponse;
	}
}