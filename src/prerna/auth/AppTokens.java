package prerna.auth;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Hashtable;
import java.util.List;
import java.util.Properties;

import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;

import prerna.om.AbstractValueObject;
import prerna.security.AbstractHttpHelper;
import prerna.util.git.GitRepoUtils;

public class AppTokens extends AbstractValueObject{
	
	// name of this user in the SEMOSS system if there is one
	
	private static AppTokens app = null;
	
	private static Properties socialData = null;
	private static AccessToken twitToken = null;
	private static AccessToken googAppToken = null; 
	
	// need to have an access token store
	private Hashtable<AuthProvider, AccessToken> accessTokens = new Hashtable<AuthProvider, AccessToken>();
	
	private AppTokens() {
		
	}
	
	public static AppTokens getInstance() {
		if(app == null) {
			app = new AppTokens();
			loginGoogleApp();
			loginTwitterApp();
			
			if(googAppToken != null) {
				app.setAccessToken(googAppToken);
			}
			if(twitToken != null) {
				app.setAccessToken(twitToken);
			}
		}
		return app;
	}
	
	public static void setSocial(Properties socialData) {
		AppTokens.socialData = socialData;
	}
	
	public void setAccessToken(AccessToken value) {
		AuthProvider name = value.getProvider();
		accessTokens.put(name, value);
	}
	
	public AccessToken getAccessToken(AuthProvider name) {
		return accessTokens.get(name);
	}
	
	public void dropAccessToken(AuthProvider name) {
		accessTokens.remove(name);
	}
	
	private static void loginTwitterApp() {
		// getting the bearer token on twitter for app authentication is a lot simpler
		// need to just combine the id and secret
		// base 64 and send as authorization
		GitRepoUtils.addCertForDomain("https://twitter.com");
		
		InputStream is = null;
		InputStreamReader isr = null;
		BufferedReader rd = null;
		if(twitToken == null) {
			try {
				String prefix = "twitter_";
				String clientId = "***REMOVED***";
				String clientSecret = "***REMOVED***";
				if(socialData != null && socialData.containsKey(prefix+"client_id")) {
					clientId = socialData.getProperty(prefix+"client_id");
				}
				if(socialData != null && socialData.containsKey(prefix+"secret_key")) {
					clientSecret = socialData.getProperty(prefix+"secret_key");
				}
				
				// make a joint string
				String jointString = clientId + ":" + clientSecret;

				// encde this base 64
				String encodedJointString = new String(Base64.getEncoder().encode(jointString.getBytes()));
				CloseableHttpClient httpclient = HttpClients.createDefault();
				HttpPost httppost = new HttpPost("https://api.twitter.com/oauth2/token");
				httppost.addHeader("Authorization", "Basic " + encodedJointString);

				List<NameValuePair> paramList = new ArrayList<NameValuePair>();
				paramList.add(new BasicNameValuePair("grant_type", "client_credentials"));
				httppost.setEntity(new UrlEncodedFormEntity(paramList));

				CloseableHttpResponse authResp = httpclient.execute(httppost);

				System.out.println("Response Code " + authResp.getStatusLine().getStatusCode());

				is = authResp.getEntity().getContent();
				isr = new InputStreamReader(is);
				rd = new BufferedReader(isr);
				StringBuffer result = new StringBuffer();
				String line = "";
				while ((line = rd.readLine()) != null) {
					result.append(line);
				}

				twitToken = AbstractHttpHelper.getJAccessToken(result.toString());
				twitToken.setProvider(AuthProvider.TWITTER);
			} catch(Exception ex) {
				ex.printStackTrace();
			} finally {
				if(is != null) {
					try {
						is.close();
					} catch(IOException e) {
						// ignore
					}
				}
				if(isr != null) {
					try {
						isr.close();
					} catch(IOException e) {
						// ignore
					}
				}
				if(rd != null) {
					try {
						rd.close();
					} catch(IOException e) {
						// ignore
					}
				}
			}
		}
	}
	
	private static void loginGoogleApp() {
		// nothing big here
		// set the name on accesstoken
		if(socialData != null && googAppToken == null) {
			googAppToken = new AccessToken();
			googAppToken.setAccess_token(socialData.getProperty("google_maps_api"));
			googAppToken.setProvider(AuthProvider.GOOGLE_MAP);
		}
	}

}
