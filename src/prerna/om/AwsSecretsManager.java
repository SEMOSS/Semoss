package prerna.om;

import java.io.IOException;
import java.util.Map;

import org.apache.http.HttpEntity;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.util.EntityUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.security.AbstractHttpHelper;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class AwsSecretsManager {

	private static final Logger classLogger = LogManager.getLogger(AwsSecretsManager.class);

	private String url;
	private boolean useApplicationCerts = false;
	private String keyStore;
	private String keyStorePass;
	private String keyPass;
	
	// inputs
	private String secretId;
	private String versionId;
	private String versionStage;
	
	// output 
	private String responseData;
	private Map<String, Object> responseJson;
	
	public AwsSecretsManager() {
		
	}
	
	/**
	 * Once inputs passed in make the request
	 */
	public void makeRequest() {
		if(this.url == null || (this.url=this.url.trim()).isEmpty()) {
			throw new NullPointerException("Must define the url");
		}
		if(this.secretId == null || (this.secretId=this.secretId.trim()).isEmpty()) {
			throw new NullPointerException("Must define the ARN of the secret key");
		}
		
		CloseableHttpResponse response = null;
		CloseableHttpClient httpClient = null;
		HttpEntity entity = null;
		try {
			httpClient = AbstractHttpHelper.getCustomClient(null, keyStore, keyStorePass, keyPass);
			HttpGet httpGet = new HttpGet(url);
			
			// add the secret id
			httpGet.addHeader("SecretId", this.secretId);
			if(this.versionId != null && !(this.versionId=this.versionId.trim()).isEmpty()) {
				httpGet.addHeader("VersionId", this.versionId);
			}
			if(this.versionStage != null && !(this.versionStage=this.versionStage.trim()).isEmpty()) {
				httpGet.addHeader("VersionStage", this.versionStage);
			}
			
			response = httpClient.execute(httpGet);
			int statusCode = response.getStatusLine().getStatusCode();
			entity = response.getEntity();
            if (statusCode >= 200 && statusCode < 300) {
                this.responseData = entity != null ? EntityUtils.toString(entity) : null;
            } else {
            	this.responseData = entity != null ? EntityUtils.toString(entity) : "";
    			throw new IllegalArgumentException("Connected to " + this.url + " but received error = " + this.responseData);
            }
            
            this.responseJson = new Gson().fromJson(this.responseData, Map.class);
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

	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
	
	public boolean isUseApplicationCerts() {
		return useApplicationCerts;
	}

	public void setUseApplicationCerts(boolean useApplicationCerts) {
		this.useApplicationCerts = useApplicationCerts;
		if(this.useApplicationCerts) {
			setKeyStore(DIHelper.getInstance().getProperty(Constants.SCHEDULER_KEYSTORE));
			setKeyStorePass(DIHelper.getInstance().getProperty(Constants.SCHEDULER_KEYSTORE_PASSWORD));
			setKeyPass(DIHelper.getInstance().getProperty(Constants.SCHEDULER_CERTIFICATE_PASSWORD));
		}
	}

	public String getKeyStore() {
		return keyStore;
	}

	public void setKeyStore(String keyStore) {
		this.keyStore = keyStore;
	}

	public String getKeyStorePass() {
		return keyStorePass;
	}

	public void setKeyStorePass(String keyStorePass) {
		this.keyStorePass = keyStorePass;
	}

	public String getKeyPass() {
		return keyPass;
	}

	public void setKeyPass(String keyPass) {
		this.keyPass = keyPass;
	}

	public String getSecretId() {
		return secretId;
	}

	public void setSecretId(String secretId) {
		this.secretId = secretId;
	}

	public String getVersionId() {
		return versionId;
	}

	public void setVersionId(String versionId) {
		this.versionId = versionId;
	}

	public String getVersionStage() {
		return versionStage;
	}

	public void setVersionStage(String versionStage) {
		this.versionStage = versionStage;
	}

	public String getResponseData() {
		return responseData;
	}

	public void setResponseData(String responseData) {
		this.responseData = responseData;
	}

	public Map<String, Object> getResponseJson() {
		return responseJson;
	}

	public void setResponseJson(Map<String, Object> responseJson) {
		this.responseJson = responseJson;
	}
	
}
