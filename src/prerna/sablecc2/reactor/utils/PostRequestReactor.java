package prerna.sablecc2.reactor.utils;

import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.http.NameValuePair;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.conn.ssl.TrustSelfSignedStrategy;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.ssl.SSLContextBuilder;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;

public class PostRequestReactor extends AbstractReactor {

	public PostRequestReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.URL.getKey(), "headersMap", "body"};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String url = this.keyValue.get(this.keysToGet[0]);
		List<Map<String, String>> headersMap = getHeadersMap();
		List<Map<String, String>> bodyMap = getBody();
	
		ResponseHandler<String> handler = new BasicResponseHandler();
		CloseableHttpResponse response = null;
		try {
			SSLContextBuilder builder = new SSLContextBuilder();
			builder.loadTrustMaterial(null, new TrustSelfSignedStrategy());
			SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(builder.build());
			CloseableHttpClient httpClient = HttpClients.custom().setSSLSocketFactory(sslsf).build();
			HttpPost httpPost = new HttpPost(url);
			if(headersMap != null && !headersMap.isEmpty()) {
				for(int i = 0; i < headersMap.size(); i++) {
					Map<String, String> head = headersMap.get(i);
					for(String key : head.keySet()) {
						httpPost.addHeader(key, head.get(key));
					}
				}
			}
			if(bodyMap != null && !bodyMap.isEmpty()) {
				List<NameValuePair> params = new ArrayList<NameValuePair>();
				for(int i = 0; i < bodyMap.size(); i++) {
					Map<String, String> body = bodyMap.get(i);
					for(String key : body.keySet()) {
						params.add(new BasicNameValuePair(key, body.get(key)));
					}
				}
				httpPost.setEntity(new UrlEncodedFormEntity(params, "UTF-8"));
			}
			response = httpClient.execute(httpPost);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		} catch (KeyStoreException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		} catch (KeyManagementException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		}
		
		String retString = null;
		try {
			retString = handler.handleResponse(response);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		}
		
		return new NounMetadata(retString, PixelDataType.CONST_STRING);
	}

	/**
	 * Get headers to add to the request
	 * @return
	 */
	private List<Map<String, String>> getHeadersMap() {
		GenRowStruct headersGrs = this.store.getNoun(this.keysToGet[1]);
		if(headersGrs != null && !headersGrs.isEmpty()) {
			List<Map<String, String>> headers = new Vector<Map<String, String>>();
			for(int i = 0; i < headersGrs.size(); i++) {
				headers.add( (Map<String, String>) headersGrs.get(i)); 
			}
			return headers;
		}
		return null;
	}
	
	/**
	 * Get headers to add to the request
	 * @return
	 */
	private List<Map<String, String>> getBody() {
		GenRowStruct bodyGrs = this.store.getNoun(this.keysToGet[2]);
		if(bodyGrs != null && !bodyGrs.isEmpty()) {
			List<Map<String, String>> body = new Vector<Map<String, String>>();
			for(int i = 0; i < bodyGrs.size(); i++) {
				body.add( (Map<String, String>) bodyGrs.get(i)); 
			}
			return body;
		}
		return null;
	}

}
