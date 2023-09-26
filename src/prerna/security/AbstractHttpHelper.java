package prerna.security;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Enumeration;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import javax.net.ssl.HostnameVerifier;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.CookieStore;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.client.methods.HttpPatch;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.client.methods.HttpPut;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustStrategy;
import org.apache.http.entity.ContentType;
import org.apache.http.entity.FileEntity;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import io.burt.jmespath.Expression;
import io.burt.jmespath.JmesPath;
import io.burt.jmespath.jackson.JacksonRuntime;
import prerna.auth.AccessToken;
import prerna.io.connector.antivirus.VirusScannerUtils;
import prerna.util.Constants;
import prerna.util.Utility;

public abstract class AbstractHttpHelper {

	private static final Logger classLogger = LogManager.getLogger(AbstractHttpHelper.class.getName());
	private static ObjectMapper mapper = new ObjectMapper();
	
	public enum POST_BODY_ENTITY {
			STRING_ENTITY, 
			BYTE_ARRAY_ENTITY,
			INPUT_STREAM_ENTITY,
			URL_ENCODED_ENTITY,
			SERIALIZABLE_ENTITY,
			FILE_ENTITY,
			};
	
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
		CloseableHttpClient httpclient = null;
		String result = null;
		try {
			// default client
			httpclient = HttpClients.createDefault();
			// this is a post
			HttpPost httppost = new HttpPost(url);
			// loop through all keys and add as basic name value pair 
			List<NameValuePair> paramList = new ArrayList<NameValuePair>();
			params.keySet().stream().forEach(param -> paramList.add(new BasicNameValuePair(param, params.get(param))));
			// set within post
			httppost.setEntity(new UrlEncodedFormEntity(paramList));

			CloseableHttpResponse response = httpclient.execute(httppost);
			int status = response.getStatusLine().getStatusCode();
			classLogger.info("Request for access token at " + url + " returned status code = " + status);

			result = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);
			classLogger.info("Request response = " + Utility.cleanLogString(result));
			
			// this will set the token to use
			if(status == 200 && extract) {
				if(json) {
					tok = getJAccessToken(result);
				} else {
					tok = getAccessToken(result);
				}
			}
		} catch (UnsupportedEncodingException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (ClientProtocolException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (UnsupportedOperationException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(httpclient != null) {
				try {
					httpclient.close();
				} catch(IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		if(tok != null && tok.getAccess_token() == null) {
			classLogger.warn("Error occurred grabbing the access token: " + Utility.cleanLogString(result));
		}
		
		// send back the token
		return tok;
	}
	
	
	/**
	 * Make a request to get an id token
	 * Uses hashtable for list of params
	 * @param url
	 * @param params
	 * @param json
	 * @param extract
	 * @return
	 */
	public static AccessToken getIdToken(String url, Map<String, String> params, boolean json, boolean extract) {
		//still using accessToken object
		AccessToken tok = null;
		CloseableHttpClient httpclient = null;
		String result = null;
		try {
			// default client
			httpclient = HttpClients.createDefault();
			// this is a post
			HttpPost httppost = new HttpPost(url);
			// loop through all keys and add as basic name value pair 
			List<NameValuePair> paramList = new ArrayList<NameValuePair>();
			params.keySet().stream().forEach(param -> paramList.add(new BasicNameValuePair(param, params.get(param))));
			// set within post
			httppost.setEntity(new UrlEncodedFormEntity(paramList));

			CloseableHttpResponse response = httpclient.execute(httppost);
			int status = response.getStatusLine().getStatusCode();
			classLogger.info("Request for access token at " + url + " returned status code = " + status);

			result = IOUtils.toString(response.getEntity().getContent(), StandardCharsets.UTF_8);
			classLogger.info("Request response = " + Utility.cleanLogString(result));
			
			// this will set the token to use
			if(status == 200 && extract) {
				if(json) {
					tok = getJIDToken(result);
				} else {
					tok = getIDToken(result);
				}
			}
		} catch (UnsupportedEncodingException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (ClientProtocolException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (UnsupportedOperationException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(httpclient != null) {
				try {
					httpclient.close();
				} catch(IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		if(tok != null && tok.getAccess_token() == null) {
			classLogger.warn("Error occurred grabbing the id token: " + Utility.cleanLogString(result));
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
	 * Get id token from a basic string
	 * @param input
	 * @return
	 */
	public static AccessToken getIDToken(String input) {
		return getAccessToken(input, "id_token");
	}
	
	/**
	 * Get access token from a basic string
	 * Example: access_token=2577b7a6ef68c2a736bf0648ea024b0f4d10e32d&scope=public_repo%2Cuser&token_type=bearer
	 * @param input
	 * @param nameOfToken
	 * @return
	 */
	public static AccessToken getAccessToken(String input, String nameOfToken) {
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
	 * Get the id token from a json
	 * @param input
	 * @return
	 */
	public static AccessToken getJIDToken(String input) {
		return getJAccessToken(input, "[id_token, token_type, expires_in]");
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
			classLogger.error(Constants.STACKTRACE, e);
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
	public static String makeGetCall(String urlStr, String accessToken) {
		return makeGetCall(urlStr, accessToken, null, true);
	}

	// makes the call to every resource going forward with the specified keys as get
	public static String makeGetCall(String urlStr, String accessToken, Hashtable params, boolean auth) {
		String retString = null;
		// fill the params on the get since it is not null
		if(params != null) {
			StringBuffer urlBuf = new StringBuffer(urlStr);
			urlBuf.append("?");
			Enumeration keys = params.keys();
			boolean first = true;
			while(keys.hasMoreElements()) {
				Object key = keys.nextElement() +"";
				Object value = params.get(key);
				if(!first) {
					urlBuf.append("&");
				}
				
				try {
					urlBuf.append(key).append("=").append(URLEncoder.encode(value+"", "UTF-8"));
				} catch (UnsupportedEncodingException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
				
				first = false;
			}
			urlStr = urlBuf.toString();
		}
		
		try {
			HttpURLConnection con = null;
			URL url = new URL(urlStr);
		    con = ( HttpURLConnection )url.openConnection();
		    con.setDoInput(true);
		    con.setDoOutput(true);
		    con.setUseCaches(false);
		    con.setRequestMethod("GET");
		    con.setRequestProperty("User-Agent", "SEMOSS");
		    if(auth) {
		    	con.setRequestProperty("Authorization","Bearer " + accessToken);
		    }
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
			classLogger.error(Constants.STACKTRACE, e);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		
		return retString;
	}

	// makes the call to every resource going forward with the specified keys as get
	public static BufferedReader getHttpStream(String url_str, String accessToken, Hashtable params, boolean auth) {
		// fill the params on the get since it is not null
		if(params != null) {
			StringBuffer urlBuf = new StringBuffer(url_str);
			urlBuf.append("?");
			Enumeration keys = params.keys();
			boolean first = true;
			while(keys.hasMoreElements()) {
				Object key = keys.nextElement() +"";
				Object value = params.get(key);
				if(!first) {
					urlBuf.append("&");
				}
				
				try {
					urlBuf.append(key).append("=").append(URLEncoder.encode(value+"", "UTF-8"));
				} catch (UnsupportedEncodingException e) {
					classLogger.error(Constants.STACKTRACE, e);
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
			classLogger.error(Constants.STACKTRACE, e);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		
		return null;
	}

	// make a post call
	
	public static String makePostCall(String url, String accessToken, Object input,  boolean json) {
		CloseableHttpClient httpclient = null;
		try {
			httpclient = HttpClients.createDefault();
			HttpPost httppost = new HttpPost(url);
			httppost.addHeader("Authorization", "Bearer " + accessToken);
			httppost.addHeader("Content-Type","application/json; charset=utf-8");
			Hashtable params = null;
			List<NameValuePair> paramList = new ArrayList<NameValuePair>();
			if(!json) {
				params = (Hashtable)input;
				Enumeration<String> keys = params.keys();
				while (keys.hasMoreElements()) {
					String key = keys.nextElement();
					String value = (String) params.get(key);
					paramList.add(new BasicNameValuePair(key, value));
				}
				httppost.setEntity(new UrlEncodedFormEntity(paramList));
			}
			// this is a json input
			else {
				String inputJson = mapper.writeValueAsString(input);
				httppost.setEntity(new StringEntity(inputJson));
			}
			
			ResponseHandler<String> handler = new BasicResponseHandler();
			CloseableHttpResponse response = httpclient.execute(httppost);
			
			System.out.println("Response Code " + response.getStatusLine().getStatusCode());
			
			BufferedReader rd = new BufferedReader(new InputStreamReader( response.getEntity().getContent(), "UTF-8"));
			StringBuffer result = new StringBuffer();
			String line = "";
			while ((line = rd.readLine()) != null) {
				result.append(line);
			}
			return result.toString();
		} catch(Exception ex) {
			ex.printStackTrace();
		} finally {
			if(httpclient != null) {
		          try {
		            httpclient.close();
		          } catch(IOException e) {
		              classLogger.error(Constants.STACKTRACE, e);
		          }
		        }
		}
		
		return null;

	}
	
	public static String makeBinaryFilePutCall(String url, String accessToken, String fileName,  String localPath){
		CloseableHttpClient httpclient = null;
			try {
				httpclient = HttpClients.createDefault();
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
			} finally {
				if(httpclient != null) {
			          try {
			            httpclient.close();
			          } catch(IOException e) {
			              classLogger.error(Constants.STACKTRACE, e);
			          }
			        }
			}
			
			return null;

		}
	
	public static String makeBinaryFilePostCall(String url, String accessToken, String filename, String filepath)
	{
		CloseableHttpClient httpclient = null;
		try {
			httpclient = HttpClients.createDefault();
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
		} finally {
			if(httpclient != null) {
		          try {
		            httpclient.close();
		          } catch(IOException e) {
		              classLogger.error(Constants.STACKTRACE, e);
		          }
		        }
		}
		return null;

	}
	
	public static String makeBinaryFilePatchCall(String url, String accessToken, String filepath)
	{
		CloseableHttpClient httpclient = null;
		try {
			 httpclient = HttpClients.createDefault();
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
		} finally {
			if(httpclient != null) {
		          try {
		            httpclient.close();
		          } catch(IOException e) {
		              classLogger.error(Constants.STACKTRACE, e);
		          }
		        }
		}
		return null;

	}
	
	
	// makes the call to every resource going forward with the specified keys as post
	
	public static String [] getCodes(String queryStr) {
	 	String [] retString = new String[2];
		String[] inputCodes = queryStr.split("&");
		for(int inputIndex = 0;inputIndex < inputCodes.length;inputIndex++) {
			String thisToken = inputCodes[inputIndex];
			if(thisToken.startsWith("state")) {
				retString[1] = thisToken.replaceAll("state=", "");
			}
			if(thisToken.startsWith("code")) {
				retString[0] = thisToken.replaceAll("code=", "");
			}
		}
		
		return retString;
	}
	
	
	//////////////////////////////////////////////////////////////////
	
	
	/**
	 * Get a custom client using the info passed in
	 * @param cookieStore
	 * @param keyStore				the keystore location
	 * @param keyStorePass			the password for the keystore
	 * @param keyPass				the password for the certificate if different from the keystore password
	 * @return
	 */
	public static CloseableHttpClient getCustomClient(CookieStore cookieStore, String keyStore, String keyStorePass, String keyPass) {
		HttpClientBuilder builder = HttpClients.custom();
		if(cookieStore != null) {
			builder.setDefaultCookieStore(cookieStore);
		}
		
		TrustStrategy trustStrategy = new TrustStrategy() {
			@Override
			public boolean isTrusted(X509Certificate[] chain, String authType) throws CertificateException {
				return true;
			}
		};
		
		HostnameVerifier verifier = new NoopHostnameVerifier();
		SSLConnectionSocketFactory connFactory = null;
		try {
			SSLContextBuilder sslContextBuilder = SSLContextBuilder.create().loadTrustMaterial(trustStrategy);

			// add the cert if required
			if(keyStore != null && !keyStore.isEmpty() && keyStorePass != null && !keyStorePass.isEmpty()) {
				File keyStoreF = new File(keyStore);
				if(!keyStoreF.exists() && !keyStoreF.isFile()) {
					classLogger.warn("Defined a keystore to use in the request but the file " + keyStoreF.getAbsolutePath() + " does not exist");
				} else {
					if(keyPass == null || keyPass.isEmpty()) {
						sslContextBuilder.loadKeyMaterial(keyStoreF, keyStorePass.toCharArray(), keyStorePass.toCharArray());
					} else {
						sslContextBuilder.loadKeyMaterial(keyStoreF, keyStorePass.toCharArray(), keyPass.toCharArray());
					}
				}
			}
			
			connFactory = new SSLConnectionSocketFactory(
					sslContextBuilder.build()
					, new String[] {"TLSv1", "TLSv1.1", "TLSv1.2"}
					, null
					, verifier) 
//			{
//				@Override
//				protected void prepareSocket(SSLSocket socket) {
//		            socket.setEnabledProtocols(new String[] { "TLSv1", "TLSv1.1", "TLSv1.2", "TLSv1.3" });
//				}
//			}
			;
		} catch (KeyManagementException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (NoSuchAlgorithmException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (KeyStoreException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (UnrecoverableKeyException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (CertificateException e) {
			classLogger.error(Constants.STACKTRACE, e);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
		}
		
		builder.setSSLSocketFactory(connFactory);
		return builder.build();
	}
	
	/**
	 * 
	 * @param url
	 * @param headerMap
	 * @param keyStore
	 * @param keyStorePass
	 * @param keyPass
	 * @return
	 */
	public static String getRequest(String url, Map<String, String> headerMap, String keyStore, String keyStorePass, String keyPass) {
		CloseableHttpResponse response = null;
		CloseableHttpClient httpClient = null;
		HttpEntity entity = null;
		String responseData = null;
		try {
			httpClient = AbstractHttpHelper.getCustomClient(null, keyStore, keyStorePass, keyPass);
			HttpGet httpGet = new HttpGet(url);
			if(headerMap != null && !headerMap.isEmpty()) {
				for(String key : headerMap.keySet()) {
					httpGet.addHeader(key, headerMap.get(key));
				}
			}
			response = httpClient.execute(httpGet);
			int statusCode = response.getStatusLine().getStatusCode();
			entity = response.getEntity();
            if (statusCode >= 200 && statusCode < 300) {
                responseData = entity != null ? EntityUtils.toString(entity) : null;
            } else {
                responseData = entity != null ? EntityUtils.toString(entity) : "";
    			throw new IllegalArgumentException("Connected to " + url + " but received error = " + responseData);
            }
			
    		return responseData;
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		} finally {
			if(entity != null) {
				try {
					EntityUtils.consume(entity);
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if(response != null) {
				try {
					response.close();
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

	/**
	 * 
	 * @param url
	 * @param headerMap
	 * @param keyStore
	 * @param keyStorePass
	 * @param keyPass
	 * @param saveFilePath
	 * @param saveFileName
	 * @return
	 */
	public static File getRequestFileDownload(String url, Map<String, String> headerMap, String keyStore, String keyStorePass, String keyPass, String saveFilePath, String saveFileName) {
		String fileName = saveFileName;
		if(fileName == null) {
			// if not passed in, see if we can grab it from the URL
			String[] pathSeparated = url.split("/");
			fileName = pathSeparated[pathSeparated.length - 1];
			if (fileName == null) {
				throw new IllegalArgumentException("Url path does not end in a file name");
			}
		}
		
		CloseableHttpResponse response = null;
		CloseableHttpClient httpClient = null;
		InputStream is = null;
		// used if virus scanning
		ByteArrayOutputStream baos = null;
		ByteArrayInputStream bais = null;
		HttpEntity entity = null;

		try {
			httpClient = AbstractHttpHelper.getCustomClient(null, keyStore, keyStorePass, keyPass);
			HttpGet httpGet = new HttpGet(url);
			if(headerMap != null && !headerMap.isEmpty()) {
				for(String key : headerMap.keySet()) {
					httpGet.addHeader(key, headerMap.get(key));
				}
			}
			response = httpClient.execute(httpGet);
			
			File fileDir = new File(saveFilePath);
			if (!fileDir.exists()) {
				Boolean success = fileDir.mkdirs();
				if(!success) {
					classLogger.warn("Unable to make the directory to save the file at location: " + Utility.cleanLogString(saveFilePath));
					throw new IllegalArgumentException("Directory to save the file download does not exist and could not be created");
				}
			}
			
			String fileLocation = Utility.getUniqueFilePath(saveFilePath, fileName);
			File savedFile = new File(fileLocation);

			entity = response.getEntity(); 
			is = entity.getContent();
			
			if (Utility.isVirusScanningEnabled()) {
				try {
					baos = new ByteArrayOutputStream();
		            IOUtils.copy(is, baos);
		            bais = new ByteArrayInputStream(baos.toByteArray());
		            
					Map<String, Collection<String>> viruses = VirusScannerUtils.getViruses(fileName, bais);
					if (!viruses.isEmpty()) {	
						String error = "File contained " + viruses.size() + " virus";
						if (viruses.size() > 1) {
							error = error + "es";
						}
						
						throw new IllegalArgumentException(error);
					}
					
					bais.reset();
					FileUtils.copyInputStreamToFile(bais, savedFile);
				} catch (IOException e) {
					throw new IllegalArgumentException("Could not read file item.", e);
				}
			} else {
				FileUtils.copyInputStreamToFile(is, savedFile);
			}
			
			return savedFile;
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		} finally {
			if(is != null) {
				IOUtils.closeQuietly(is);
			}
			if(bais != null) {
				IOUtils.closeQuietly(bais);
			}
			if(baos != null) {
				IOUtils.closeQuietly(baos);
			}
			if(entity != null) {
				try {
					EntityUtils.consume(entity);
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
			if(response != null) {
				try {
					response.close();
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
	
	/**
	 * 
	 * @param url
	 * @param headersMap
	 * @param bodyMap
	 * @param keyStore
	 * @param keyStorePass
	 * @param keyPass
	 * @return
	 */
	public static String postRequestUrlEncodedBody(String url, Map<String, String> headersMap, Map<String, String> bodyMap, String keyStore, String keyStorePass, String keyPass) {
        String responseData = null;
		CloseableHttpClient httpClient = null;
		CloseableHttpResponse response = null;
		HttpEntity entity = null;
		try {
			httpClient = AbstractHttpHelper.getCustomClient(null, keyStore, keyStorePass, keyPass);
			HttpPost httpPost = new HttpPost(url);
			if(headersMap != null && !headersMap.isEmpty()) {
				for(String key : headersMap.keySet()) {
					httpPost.addHeader(key, headersMap.get(key));
				}
			}
			if(bodyMap != null && !bodyMap.isEmpty()) {
				List<NameValuePair> params = new ArrayList<NameValuePair>();
				for(String key : bodyMap.keySet()) {
					params.add(new BasicNameValuePair(key, bodyMap.get(key)));
				}
				httpPost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
			}
			response = httpClient.execute(httpPost);
			
			int statusCode = response.getStatusLine().getStatusCode();
			entity = response.getEntity();
            if (statusCode >= 200 && statusCode < 300) {
                responseData = entity != null ? EntityUtils.toString(entity) : null;
            } else {
                responseData = entity != null ? EntityUtils.toString(entity) : "";
    			throw new IllegalArgumentException("Connected to " + url + " but received error = " + responseData);
            }
			
    		return responseData;
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		}
	}
	
	/**
	 * 
	 * @param url
	 * @param headersMap
	 * @param body
	 * @param contentType
	 * @param keyStore
	 * @param keyStorePass
	 * @param keyPass
	 * @return
	 */
	public static String postRequestStringBody(String url, Map<String, String> headersMap, String body, ContentType contentType, String keyStore, String keyStorePass, String keyPass) {
        String responseData = null;
		CloseableHttpClient httpClient = null;
		CloseableHttpResponse response = null;
		HttpEntity entity = null;
		try {
			httpClient = AbstractHttpHelper.getCustomClient(null, keyStore, keyStorePass, keyPass);
			HttpPost httpPost = new HttpPost(url);
			if(headersMap != null && !headersMap.isEmpty()) {
				for(String key : headersMap.keySet()) {
					httpPost.addHeader(key, headersMap.get(key));
				}
			}
			if(body != null && !body.isEmpty()) {
				httpPost.setEntity(new StringEntity(body, contentType));
			}
			response = httpClient.execute(httpPost);
			
			int statusCode = response.getStatusLine().getStatusCode();
			entity = response.getEntity();
            if (statusCode >= 200 && statusCode < 300) {
                responseData = entity != null ? EntityUtils.toString(entity) : null;
            } else {
                responseData = entity != null ? EntityUtils.toString(entity) : "";
    			throw new IllegalArgumentException("Connected to " + url + " but received error = " + responseData);
            }
			
    		return responseData;
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		}
	}
}
