package prerna.aws;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.security.AbstractHttpHelper;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class AwsSecretsManager {

	private static final Logger classLogger = LogManager.getLogger(AwsSecretsManager.class);

	// https://docs.aws.amazon.com/secretsmanager/latest/userguide/endpoints.html
	private String url;
	private boolean useApplicationCerts = false;
	private String keyStore;
	private String keyStorePass;
	private String keyPass;

	// security
	private String accessKey;
	private String secretKey;
	
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
			throw new NullPointerException("Must define the ARN of the secret");
		}
		
		Map<String, String> headersMap = new HashMap<>();
		headersMap.put("SecretId", this.secretId);
		if(this.versionId != null && !(this.versionId=this.versionId.trim()).isEmpty()) {
			headersMap.put("VersionId", this.versionId);
		}
		if(this.versionStage != null && !(this.versionStage=this.versionStage.trim()).isEmpty()) {
			headersMap.put("VersionStage", this.versionStage);
		}
		if(this.accessKey != null && this.secretKey != null) {
			String authorization = createAuthorizationHeader(accessKey, secretKey);
			headersMap.put("Authorization", authorization);
		}

		this.responseData = AbstractHttpHelper.getRequest(url, headersMap, keyStore, keyStorePass, keyPass);
		this.responseJson = new Gson().fromJson(this.responseData, Map.class);
	}

	/**
	 * 
	 * @param accessKey
	 * @param secretKey
	 * @return
	 */
	private static String createAuthorizationHeader(String accessKey, String secretKey) {
		String credentials = accessKey + ":" + secretKey;
		String encodedCredentials = Base64.getEncoder().encodeToString(credentials.getBytes(StandardCharsets.UTF_8));
		return "Basic " + encodedCredentials;
	}

	//////////////////////////////////


	public String getUrl() {
		return url;
	}

	public void setUrl(String url) {
		this.url = url;
	}
	
	public String getAccessKey() {
		return accessKey;
	}

	public void setAccessKey(String accessKey) {
		this.accessKey = accessKey;
	}

	public String getSecretKey() {
		return secretKey;
	}

	public void setSecretKey(String secretKey) {
		this.secretKey = secretKey;
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
