package prerna.security;

import io.burt.jmespath.Expression;
import io.burt.jmespath.JmesPath;
import io.burt.jmespath.jackson.JacksonRuntime;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;

import org.apache.http.Consts;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;

import prerna.auth.AccessToken;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public abstract class AbstractHttpHelper 
{

	static ObjectMapper mapper = new ObjectMapper();

	
	// makes a request to get access token with a params hashtable
	public static AccessToken getAccessToken(String url, Hashtable params, boolean json, boolean extract)
	{
		String accessToken = null;
		AccessToken tok = new AccessToken();
		try {
			CloseableHttpClient httpclient = HttpClients.createDefault();
			HttpPost httppost = new HttpPost(url);

			List<NameValuePair> paramList = new ArrayList<NameValuePair>();
			Enumeration<String> keys = params.keys();
			
			int paramIndex = 0;
			
			while (keys.hasMoreElements()) {
				String key = keys.nextElement();
				String value = (String) params.get(key);
				paramList.add(new BasicNameValuePair(key, value));
				paramIndex++;
			}

			
			httppost.setEntity(new UrlEncodedFormEntity(paramList));

			ResponseHandler<String> handler = new BasicResponseHandler();
			CloseableHttpResponse response = httpclient.execute(httppost);
			
			System.out.println("Response Code " + response.getStatusLine().getStatusCode());
			
			int status = response.getStatusLine().getStatusCode();
			
			BufferedReader rd = new BufferedReader(
			        new InputStreamReader(response.getEntity().getContent()));

			StringBuffer result = new StringBuffer();
			String line = "";
			while ((line = rd.readLine()) != null) {
				result.append(line);
			}
			
			System.out.println("Result ..  " + result);
			accessToken = result.toString();
			
			tok.setAccess_token(accessToken);
			
			if(status == 200 && extract)
			{
				if(json)
					tok = getJAccessToken(result.toString());
				else
					tok = getAccessToken(result.toString());
					
			}
			
			
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (ClientProtocolException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (UnsupportedOperationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		System.out.println("Forcing redo");

		return tok;
	}
	
	public static AccessToken getAccessToken(String input)
	{
		return getAccessToken(input, "access_token");
	}
	
	// processes to get he access token as simple
	public static AccessToken getAccessToken(String input, String nameOfToken)
	{
		String accessToken = null;
		
		// access_token=2577b7a6ef68c2a736bf0648ea024b0f4d10e32d&scope=public_repo%2Cuser&token_type=bearer
		String [] tokens = input.split("&");
		for(int tokenIndex = 0;tokenIndex < tokens.length;tokenIndex++)
		{
			String thisToken = tokens[tokenIndex];
			if(thisToken.startsWith(nameOfToken))
				accessToken = thisToken.replaceAll(nameOfToken + "=", "");
		}
		AccessToken tok = new AccessToken();
		tok.setAccess_token(accessToken);
		tok.init();
		
		return tok;
	
	}

	public static AccessToken getJAccessToken(String input)
	{
		return getJAccessToken(input, "[access_token, token_type, expires_in]");
	}
	
	// processes to get the access token as json
	public static AccessToken getJAccessToken(String json, String nameOfToken)
	{
		JsonNode result;
		String retString = null;
		try {
			JmesPath<JsonNode> jmespath = new JacksonRuntime();
			// Expressions need to be compiled before you can search. Compiled expressions
			// are reusable and thread safe. Compile your expressions once, just like database
			// prepared statements.
			Expression<JsonNode> expression = jmespath.compile(nameOfToken);

			//AccessToken tok = mapper.readValue(json, AccessToken.class);
			AccessToken tok = new AccessToken();
			JsonNode input = mapper.readTree(json);
			result = expression.search(input);
			if(result.size() >= 0)
				tok.setAccess_token(result.get(0).asText());
			if(result.size() >= 1)
				tok.setToken_type(result.get(1).asText());
			if(result.size() >= 2)
				tok.setExpires_in(result.get(2).asInt());
			
			tok.init();
			retString = tok.getAccess_token();
			
			return tok;
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return null;

	}

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

		    BufferedReader br = new BufferedReader(new InputStreamReader( con.getInputStream() ));
		    String str = "";
		    String line;
		    while((line = br.readLine()) != null){
		        str += line;
		    }
		    System.out.println(str);	
		    
		    retString = str;
			//System.out.println("Output.. " + retString);
			
		} catch (MalformedURLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
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

		    BufferedReader br = new BufferedReader(new InputStreamReader( con.getInputStream() ));
		    
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
			
			BufferedReader rd = new BufferedReader(
			        new InputStreamReader(response.getEntity().getContent()));

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
				
				BufferedReader rd = new BufferedReader(
				        new InputStreamReader(response.getEntity().getContent()));

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
			
			BufferedReader rd = new BufferedReader(
			        new InputStreamReader(response.getEntity().getContent()));

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
			
			BufferedReader rd = new BufferedReader(
			        new InputStreamReader(response.getEntity().getContent()));

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
		String json = "{ \"access_token\": \"ya29.GluRBVQwBs9V7oLe17RhJsFUpu_TyX2nXWs3RLuOvw1PTGX_e0VPrpas-KyqBEp5jkgthpfEyN8Qj5Xc3Rp6PxROACdF9Gz8nM1F7T76UeMDo2HbSnvVPEvgQOY8\", \"token_type\": \"Bearer\", \"expires_in\": 3600}";
		AbstractHttpHelper.getJAccessToken(json);
	}	
}
