package prerna.sablecc2.reactor.utils;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.CloseableHttpClient;

import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.security.AbstractHttpHelper;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class GetRequestReactor extends AbstractReactor {

	public GetRequestReactor() {
		this.keysToGet = new String[]{ReactorKeysEnum.URL.getKey(), "headersMap", "useApplicationCert"};
	}
	
	@Override
	public NounMetadata execute() {
		organizeKeys();
		String url = this.keyValue.get(this.keysToGet[0]);
		List<Map<String, String>> headersMap = getHeadersMap();
		String keyStore = null;
		String keyStorePass = null;
		boolean useApplicationCert = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[2]) + "");
		if(useApplicationCert) {
			keyStore = DIHelper.getInstance().getProperty(Constants.SCHEDULER_KEYSTORE);
			keyStorePass = DIHelper.getInstance().getProperty(Constants.SCHEDULER_KEYSTORE_PASSWORD);
		}
		
		ResponseHandler<String> handler = new BasicResponseHandler();
		CloseableHttpResponse response = null;
		try {
			CloseableHttpClient httpClient = AbstractHttpHelper.getCustomClient(null, keyStore, keyStorePass);
			HttpGet httpGet = new HttpGet(url);
			if(headersMap != null && !headersMap.isEmpty()) {
				for(int i = 0; i < headersMap.size(); i++) {
					Map<String, String> head = headersMap.get(i);
					for(String key : head.keySet()) {
						httpGet.addHeader(key, head.get(key));
					}
				}
			}
			response = httpClient.execute(httpGet);
		} catch (IOException e) {
			e.printStackTrace();
			throw new IllegalArgumentException("Could not connect to URL at " + url);
		}
		
		String retString = null;
		try {
			retString = handler.handleResponse(response);
		} catch (IOException e) {
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
	
	@Override
	protected String getDescriptionForKey(String key) {
		if(key.equals("headersMap")) {
			return "Map containing key-value pairs to send in the GET request";
		} else if(key.equals("useApplicationCert")) {
			return "Boolean if we should use the default application certificate when making the request";
		}
		return super.getDescriptionForKey(key);
	}

}
