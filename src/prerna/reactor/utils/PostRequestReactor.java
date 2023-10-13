package prerna.reactor.utils;

import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
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
		Map<String, String> headersMap = getHeadersMap();
		Map<String, String> bodyMap = getBody();
		String keyStore = null;
		String keyStorePass = null;
		String keyPass = null;
		boolean useApplicationCert = Boolean.parseBoolean(this.keyValue.get(this.keysToGet[3]) + "");
		if(useApplicationCert) {
			keyStore = DIHelper.getInstance().getProperty(Constants.SCHEDULER_KEYSTORE);
			keyStorePass = DIHelper.getInstance().getProperty(Constants.SCHEDULER_KEYSTORE_PASSWORD);
			keyPass = DIHelper.getInstance().getProperty(Constants.SCHEDULER_CERTIFICATE_PASSWORD);
		}
		
		return new NounMetadata(AbstractHttpHelper.postRequestUrlEncodedBody(url, headersMap, bodyMap, keyStore, keyStorePass, keyPass), PixelDataType.CONST_STRING);
	}

	/**
	 * Get headers to add to the request
	 * @return
	 */
	private Map<String, String> getHeadersMap() {
		GenRowStruct headersGrs = this.store.getNoun(this.keysToGet[1]);
		if(headersGrs != null && !headersGrs.isEmpty()) {
			Map<String, String> headers = new HashMap<>();
			for(int i = 0; i < headersGrs.size(); i++) {
				headers.putAll( (Map<String, String>) headersGrs.get(i)); 
			}
			return headers;
		}
		return null;
	}
	
	/**
	 * Get headers to add to the request
	 * @return
	 */
	private Map<String, String> getBody() {
		GenRowStruct bodyGrs = this.store.getNoun(this.keysToGet[2]);
		if(bodyGrs != null && !bodyGrs.isEmpty()) {
			Map<String, String> body = new HashMap<>();
			for(int i = 0; i < bodyGrs.size(); i++) {
				body.putAll( (Map<String, String>) bodyGrs.get(i)); 
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
