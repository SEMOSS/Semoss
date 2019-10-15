package prerna.security;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.burt.jmespath.Expression;
import io.burt.jmespath.JmesPath;
import io.burt.jmespath.jackson.JacksonRuntime;
import prerna.auth.AccessToken;

public abstract class AbstractHttpHelper {

	private static final Logger LOGGER = Logger.getLogger(AbstractHttpHelper.class.getName());
	private static ObjectMapper mapper = new ObjectMapper();
	
	/////////////////////////////////////////////////////
	/////////////////////////////////////////////////////
	/////////////////////////////////////////////////////
	
	/*
	 * Methods for generating an access token
	 */
	
	/**
	 * Make a request to get an access token
	 * Uses hashtable for list of params
	 * @param url
	 * @param params
	 * @param json
	 * @param extract
	 * @return
	 */
	public static AccessToken getAccessToken(String url, Map<String, String> params, boolean json, boolean extract) {
		AccessToken tok = null;
		try {
			// default client
			CloseableHttpClient httpclient = HttpClients.createDefault();
			// this is a post
			HttpPost httppost = new HttpPost(url);
			// loop through all keys and add as basic name value pair 
			List<NameValuePair> paramList = new ArrayList<NameValuePair>();
			params.keySet().stream().forEach(param -> paramList.add(new BasicNameValuePair(param, params.get(param))));
			// set within post
			httppost.setEntity(new UrlEncodedFormEntity(paramList));

			CloseableHttpResponse response = httpclient.execute(httppost);
			int status = response.getStatusLine().getStatusCode();
			LOGGER.info("Request for access token at " + url + " returned status code = " + status);

			String result = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);
			LOGGER.debug("Request response = " + result);

			// this will set the token to use
			if(status == 200 && extract) {
				if(json) {
					tok = getJAccessToken(result.toString());
				} else {
					tok = getAccessToken(result.toString());
				}
			}
		} catch (UnsupportedEncodingException e) {
			e.printStackTrace();
		} catch (ClientProtocolException e) {
			e.printStackTrace();
		} catch (UnsupportedOperationException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}

		// send back the token
		return tok;
	}
	
	/**
	 * Get access token from a basic string
	 * @param input
	 * @return
	 */
	public static AccessToken getAccessToken(String input) {
		return getAccessToken(input, "access_token");
	}
	
	/**
	 * Get access token from a basic string
	 * Example: access_token=2577b7a6ef68c2a736bf0648ea024b0f4d10e32d&scope=public_repo%2Cuser&token_type=bearer
	 * @param input
	 * @param nameOfToken
	 * @return
	 */
	public static AccessToken getAccessToken(String input, String nameOfToken)
	{
		String accessToken = null;
		String [] tokens = input.split("&");
		for(int tokenIndex = 0;tokenIndex < tokens.length;tokenIndex++) {
			String thisToken = tokens[tokenIndex];
			if(thisToken.startsWith(nameOfToken)) {
				accessToken = thisToken.replaceAll(nameOfToken + "=", "");
				break;
			}
		}
		AccessToken tok = new AccessToken();
		tok.setAccess_token(accessToken);
		tok.init();
		
		return tok;
	}

	/**
	 * Get the access token from a json
	 * @param input
	 * @return
	 */
	public static AccessToken getJAccessToken(String input) {
		return getJAccessToken(input, "[access_token, token_type, expires_in]");
	}
	
	/**
	 * Get the access token from a json
	 * @param json
	 * @param nameOfToken
	 * @return
	 */
	public static AccessToken getJAccessToken(String json, String nameOfToken) {
		AccessToken tok = new AccessToken();

		try {
			JmesPath<JsonNode> jmespath = new JacksonRuntime();
			// Expressions need to be compiled before you can search. Compiled expressions
			// are reusable and thread safe
			// Compile your expressions once, just like database prepared statements.
			Expression<JsonNode> expression = jmespath.compile(nameOfToken);

			JsonNode input = mapper.readTree(json);
			JsonNode result = expression.search(input);
			if(result.size() >= 0) {
				tok.setAccess_token(result.get(0).asText());
			}
			if(result.size() >= 1) {
				tok.setToken_type(result.get(1).asText());
			}
			if(result.size() >= 2) {
				tok.setExpires_in(result.get(2).asInt());
			}
			tok.init();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return tok;
	}
	
	
	/////////////////////////////////////////////////////
	/////////////////////////////////////////////////////
	/////////////////////////////////////////////////////

	/*
	 * Methods for making requests using the access token
	 */


	// makes the call to every resource going forward with the specified keys as get
	public static String makeGetCall(String url_str, String accessToken)
	{
		return makeGetCall(url_str, accessToken, null, true);
	}

	// makes the call to every resource going forward with the specified keys as get
	public static String makeGetCall(String url_str, String accessToken, Hashtable params, boolean auth)
	{
		String retString = null;
		
		// fill the params on the get since it is not null
		if(params != null)
		{
			StringBuffer urlBuf = new StringBuffer(url_str);
			urlBuf.append("?");
			Enumeration keys = params.keys();
			boolean first = true;
			while(keys.hasMoreElements())
			{
				Object key = keys.nextElement() +"";
				Object value = params.get(key);
				
				if(!first)
					urlBuf.append("&");
				
				try {
					urlBuf.append(key).append("=").append(URLEncoder.encode(value+"", "UTF-8"));
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				first = false;
			}
			url_str = urlBuf.toString();
		}
		
		try {
			
			HttpURLConnection con = null;
			URL url = new URL(url_str);
		    con = ( HttpURLConnection )url.openConnection();
		    con.setDoInput(true);
		    con.setDoOutput(true);
		    con.setUseCaches(false);
		    con.setRequestMethod("GET");
		    con.setRequestProperty("User-Agent", "SEMOSS");
		    if(auth)
		    	con.setRequestProperty("Authorization","Bearer " + accessToken);
		    con.setRequestProperty("Accept","application/json"); // I added this line.
		    con.connect();

		    BufferedReader br = new BufferedReader(new InputStreamReader( con.getInputStream() , "UTF-8"));
		    StringBuilder str = new StringBuilder();
		    String line;
		    while((line = br.readLine()) != null){
		        str.append(line);
		    }
		    retString = str.toString();
		    System.out.println(retString);	
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		return retString;
	}

	// makes the call to every resource going forward with the specified keys as get
	public static BufferedReader getHttpStream(String url_str, String accessToken, Hashtable params, boolean auth)
	{
		String retString = null;
		
		// fill the params on the get since it is not null
		if(params != null)
		{
			StringBuffer urlBuf = new StringBuffer(url_str);
			urlBuf.append("?");
			Enumeration keys = params.keys();
			boolean first = true;
			while(keys.hasMoreElements())
			{
				Object key = keys.nextElement() +"";
				Object value = params.get(key);
				
				if(!first)
					urlBuf.append("&");
				
				try {
					urlBuf.append(key).append("=").append(URLEncoder.encode(value+"", "UTF-8"));
				} catch (UnsupportedEncodingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
				first = false;
			}
			url_str = urlBuf.toString();
		}
		
		try {
			
			HttpURLConnection con = null;
			URL url = new URL(url_str);
		    con = ( HttpURLConnection )url.openConnection();
		    con.setDoInput(true);
		    con.setDoOutput(true);
		    con.setUseCaches(false);
		    con.setRequestMethod("GET");
		    con.setRequestProperty("User-Agent", "SEMOSS");
		    if(auth)
		    	con.setRequestProperty("Authorization","Bearer " + accessToken);
		    con.setRequestProperty("Accept","application/json"); // I added this line.
		    con.connect();

		    BufferedReader br = new BufferedReader(new InputStreamReader( con.getInputStream(), "UTF-8" ));
		    
		    return br;
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return null;
	}

	// make a post call
	
	public static String makePostCall(String url, String accessToken, Object input,  boolean json)
	{
		
		try {
			CloseableHttpClient httpclient = HttpClients.createDefault();
			HttpPost httppost = new HttpPost(url);
			httppost.addHeader("Authorization", "Bearer " + accessToken);
			httppost.addHeader("Content-Type","application/json; charset=utf-8");
			Hashtable params = null;
			List<NameValuePair> paramList = new ArrayList<NameValuePair>();
			if(!json)
			{
				params = (Hashtable)input;

				Enumeration<String> keys = params.keys();
				
				int paramIndex = 0;
				
				while (keys.hasMoreElements()) {
					String key = keys.nextElement();
					String value = (String) params.get(key);
					paramList.add(new BasicNameValuePair(key, value));
					paramIndex++;
				}
				httppost.setEntity(new UrlEncodedFormEntity(paramList));

			}
			else // this is a json input
			{
				String inputJson = mapper.writeValueAsString(input);
				httppost.setEntity(new StringEntity(inputJson));
			}
			
			ResponseHandler<String> handler = new BasicResponseHandler();
			CloseableHttpResponse response = httpclient.execute(httppost);
			
			System.out.println("Response Code " + response.getStatusLine().getStatusCode());
			
			int status = response.getStatusLine().getStatusCode();
			BufferedReader rd = new BufferedReader(new InputStreamReader( response.getEntity().getContent(), "UTF-8"));
			StringBuffer result = new StringBuffer();
			String line = "";
			while ((line = rd.readLine()) != null) {
				result.append(line);
			}
			return result.toString();
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
		
		return null;

	}
	
	public static String makeBinaryFilePutCall(String url, String accessToken, String fileName,  String localPath){
			
			try {
				CloseableHttpClient httpclient = HttpClients.createDefault();
				HttpPut httpput = new HttpPut(url);
				httpput.addHeader("Authorization", "Bearer " + accessToken);
				httpput.addHeader("Content-Type","application/json; charset=utf-8");
				File fileupload = new File(localPath);
				httpput.setEntity(new FileEntity(fileupload));
				ResponseHandler<String> handler = new BasicResponseHandler();
				CloseableHttpResponse response = httpclient.execute(httpput);
				
				System.out.println("Response Code " + response.getStatusLine().getStatusCode());
				
				int status = response.getStatusLine().getStatusCode();
				
				BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), "UTF-8"));

				StringBuffer result = new StringBuffer();
				String line = "";
				while ((line = rd.readLine()) != null) {
					result.append(line);
				}
				return result.toString();
			}catch(Exception ex)
			{
				ex.printStackTrace();
			}
			
			return null;

		}
	
	public static String makeBinaryFilePostCall(String url, String accessToken, String filename, String filepath)
	{
		
		try {
			CloseableHttpClient httpclient = HttpClients.createDefault();
			HttpPost httppost = new HttpPost(url);
			httppost.addHeader("Authorization", "Bearer " + accessToken);
			httppost.addHeader("Content-Type","application/octet-stream");
			httppost.addHeader("Dropbox-API-Arg","{\"path\": \"/"+filename+"\",\"mode\": \"add\",\"autorename\": true,\"mute\": false}");
			File fileupload = new File(filepath);
			httppost.setEntity(new FileEntity(fileupload));
			ResponseHandler<String> handler = new BasicResponseHandler();
			CloseableHttpResponse response = httpclient.execute(httppost);
			
			System.out.println("Response Code " + response.getStatusLine().getStatusCode());
			
			int status = response.getStatusLine().getStatusCode();
			
			BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), "UTF-8"));
			StringBuffer result = new StringBuffer();
			String line = "";
			while ((line = rd.readLine()) != null) {
				result.append(line);
			}
			return result.toString();
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
		return null;

	}
	
	public static String makeBinaryFilePatchCall(String url, String accessToken, String filepath)
	{
		
		try {
			CloseableHttpClient httpclient = HttpClients.createDefault();
			HttpPatch httppatch = new HttpPatch(url);
			httppatch.addHeader("Authorization", "Bearer " + accessToken);

			File fileupload = new File(filepath);
			httppatch.setEntity(new FileEntity(fileupload));
			ResponseHandler<String> handler = new BasicResponseHandler();
			CloseableHttpResponse response = httpclient.execute(httppatch);
			
			System.out.println("Response Code " + response.getStatusLine().getStatusCode());
			
			int status = response.getStatusLine().getStatusCode();
			
			BufferedReader rd = new BufferedReader(new InputStreamReader(response.getEntity().getContent(), "UTF-8"));

			StringBuffer result = new StringBuffer();
			String line = "";
			while ((line = rd.readLine()) != null) {
				result.append(line);
			}
			return result.toString();
		}catch(Exception ex)
		{
			ex.printStackTrace();
		}
		return null;

	}
	
	
	// makes the call to every resource going forward with the specified keys as post
	
	public static String [] getCodes(String queryStr)
	{
		String [] retString = new String[2];
		
		String[] inputCodes = queryStr.split("&");
		String state = null, newcode = null;
		for(int inputIndex = 0;inputIndex < inputCodes.length;inputIndex++)
		{
			String thisToken = inputCodes[inputIndex];
			if(thisToken.startsWith("state"))
				retString[1] = thisToken.replaceAll("state=", "");
			if(thisToken.startsWith("code"))
				retString[0] = thisToken.replaceAll("code=", "");
		}
		
		return retString;

	}
	
	public static void main(String [] args)
	{
		String json = "{ \"access_token\": \"***REMOVED***\", \"token_type\": \"Bearer\", \"expires_in\": 3600}";
		AbstractHttpHelper.getJAccessToken(json);
	}	
}
