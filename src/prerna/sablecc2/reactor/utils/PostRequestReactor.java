package prerna.sablecc2.reactor.utils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.http.HttpEntity;
import org.apache.http.NameValuePair;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.security.AbstractHttpHelper;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public class PostRequestReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(PostRequestReactor.class);

	public PostRequestReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.URL.getKey(), "headersMap", "bodyMap", 
				ReactorKeysEnum.USE_APPLICATION_CERT.getKey()};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String url = this.keyValue.get(this.keysToGet[0]);
		Utility.checkIfValidDomain(url);
		List<Map<String, String>> headersMap = getHeadersMap();
		List<Map<String, String>> bodyMap = getBody();
		String keyStore = null;
		String keyStorePass = null;
		String keyPass = null;
		boolean useApplicationCert = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[3]) + "");
		if(useApplicationCert) {
			keyStore = DIHelper.getInstance().getProperty(Constants.SCHEDULER_KEYSTORE);
			keyStorePass = DIHelper.getInstance().getProperty(Constants.SCHEDULER_KEYSTORE_PASSWORD);
			keyPass = DIHelper.getInstance().getProperty(Constants.SCHEDULER_CERTIFICATE_PASSWORD);
		}
		
        String responseData = null;
		CloseableHttpClient httpClient = null;
		CloseableHttpResponse response = null;
		HttpEntity entity = null;
		try {
			httpClient = AbstractHttpHelper.getCustomClient(null, keyStore, keyStorePass, keyPass);
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
			
			int statusCode = response.getStatusLine().getStatusCode();
			entity = response.getEntity();
            if (statusCode >= 200 && statusCode < 300) {
                responseData = entity != null ? EntityUtils.toString(entity) : null;
            } else {
                responseData = entity != null ? EntityUtils.toString(entity) : "";
    			throw new IllegalArgumentException("Connected to " + url + " but received error = " + responseData);
            }
			
    		return new NounMetadata(responseData, PixelDataType.CONST_STRING);
		} catch (IOException e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		}
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

	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals("headersMap")) {
			return "Map containing key-value pairs to send in the POST request";
		} else if(key.equals("bodyMap")) {
			return "Map containing key-value pairs to send in the body of the POST request";
		}
		return super.getDescriptionForKey(key);
	}
	
}
