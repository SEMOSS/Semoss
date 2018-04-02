package prerna.security;

import io.burt.jmespath.Expression;
import io.burt.jmespath.JmesPath;
import io.burt.jmespath.jackson.JacksonRuntime;

import java.io.BufferedReader;
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

import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class AbstractHttpHelper 
{

	// makes a request to get access token with a params hashtable
	public static String getAccessToken(String url, Hashtable params, boolean json, boolean extract)
	{
		String accessToken = null;
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
			
			if(status == 200 && extract)
			{
				if(json)
					accessToken = getJAccessToken(result.toString());
				else
					accessToken = getAccessToken(result.toString());
					
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

		return accessToken;
	}
	
	public static String getAccessToken(String input)
	{
		return getAccessToken(input, "access_token");
	}
	
	// processes to get he access token as simple
	public static String getAccessToken(String input, String nameOfToken)
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
		return accessToken;
	
	}

	public static String getJAccessToken(String input)
	{
		return getJAccessToken(input, "access_token");
	}
	
	// processes to get the access token as json
	public static String getJAccessToken(String json, String nameOfToken)
	{
		JsonNode result;
		String retString = null;
		try {
			JmesPath<JsonNode> jmespath = new JacksonRuntime();
			// Expressions need to be compiled before you can search. Compiled expressions
			// are reusable and thread safe. Compile your expressions once, just like database
			// prepared statements.
			Expression<JsonNode> expression = jmespath.compile(nameOfToken);

			
			ObjectMapper mapper = new ObjectMapper();
			JsonNode input = mapper.readTree(json);
			result = expression.search(input);
			
			retString = result.asText();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return retString;

	}

	// makes the call to every resource going forward with the specified keys as get
	public static String makeGetCall(String url_str, String accessToken)
	{
		return makeGetCall(url_str, accessToken, null);
	}

	// makes the call to every resource going forward with the specified keys as get
	public static String makeGetCall(String url_str, String accessToken, Hashtable params)
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
		    con.setRequestProperty("Authorization","Bearer " + accessToken);
		    con.setRequestProperty("Accept","application/json"); // I added this line.
		    con.connect();

		    BufferedReader br = new BufferedReader(new InputStreamReader( con.getInputStream() ));
		    String str = null;
		    String line;
		    while((line = br.readLine()) != null){
		        str += line;
		    }
		    System.out.println(str);			
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
	
	
}
