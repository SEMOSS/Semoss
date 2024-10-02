package prerna.graph;

import java.io.IOException;
import java.net.URLEncoder;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.AccessToken;
import prerna.util.Constants;
import prerna.util.SocialPropertiesUtil;

public class MSGraphAPICall {
	 
	private static final Logger classLogger = LogManager.getLogger(MSGraphAPICall.class);

	/**
	 *This method use to get users data from graph api with next link having subsequent userdata
	 *@param accessToken, searchTerm, nextLink
	 *@return String
	 */
	public String getUserDetails(AccessToken accessToken, String searchTerm, String nextLink) throws Exception {
		classLogger.info("getUserDetails based on Graph Api");
		CloseableHttpClient httpClient = null;
		HttpResponse response = null;
		HttpEntity entity = null;
		try {
			httpClient = HttpClients.createDefault();

			String uri = "";
			if(nextLink == null) {
				String searchParam = "";
				if(searchTerm != null && !(searchTerm=searchTerm.trim()).isEmpty()) {
					searchParam = "$search=" + 
										URLEncoder.encode("\"displayName:" + searchTerm 
										+ "\" OR \"mail:" + searchTerm
										+ "\" OR \"userPrincipalName:" + searchTerm + "\"", 
										java.nio.charset.StandardCharsets.UTF_8.toString())
									+ "&";
				}
				uri = SocialPropertiesUtil.getInstance().getProperty("ms_graph_url") + "/v1.0/users?" + searchParam + "$top=999&$count=true";
			} else {
				uri = nextLink;
			}
			
			HttpGet httpGet = new HttpGet(uri);
	        // Set headers
			httpGet.setHeader("Authorization", "Bearer " + accessToken.getAccess_token());
			httpGet.setHeader("Accept", "application/json");
			httpGet.setHeader("ConsistencyLevel", "eventual");
	 
	        // Execute the request
			response = httpClient.execute(httpGet);
			entity = response.getEntity();
			String jsonResponse = EntityUtils.toString(entity);
			return jsonResponse;
		} finally {
			if(entity != null) {
				try {
					EntityUtils.consume(entity);
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if(httpClient != null) {
				try {
					httpClient.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
}