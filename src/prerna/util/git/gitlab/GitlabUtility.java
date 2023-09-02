package prerna.util.git.gitlab;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.snowflake.client.jdbc.internal.google.gson.Gson;
import prerna.security.AbstractHttpHelper;
import prerna.util.Constants;
import prerna.util.DIHelper;

public class GitlabUtility {

	private static final Logger classLogger = LogManager.getLogger(GitlabUtility.class);

	/**
	 * 
	 * @param host
	 * @param gitProjectId
	 * @param personalOAuthToken
	 * @param privateToken
	 * @param scope
	 * @param useApplicationCert
	 * @return
	 */
	public static List<Map<String, Object>> getGitlabJobs(String host, String gitProjectId, String personalOAuthToken, String privateToken, String scope, boolean useApplicationCert) {
		String url = host;
		if(url.endsWith("/")) {
			url += "api/v4/projects/"+gitProjectId+"/jobs";
		} else {
			url += "/api/v4/projects/"+gitProjectId+"/jobs";
		}
		if(scope != null && !scope.isEmpty()) {
			url += "?scope[]="+scope;
		}
		
		String keyStore = null;
		String keyStorePass = null;
		String keyPass = null;
		if(useApplicationCert) {
			keyStore = DIHelper.getInstance().getProperty(Constants.SCHEDULER_KEYSTORE);
			keyStorePass = DIHelper.getInstance().getProperty(Constants.SCHEDULER_KEYSTORE_PASSWORD);
			keyPass = DIHelper.getInstance().getProperty(Constants.SCHEDULER_CERTIFICATE_PASSWORD);
		}
		
		List<Map<String, String>> headersMap = new ArrayList<>();
		Map<String, String> authMap = new HashMap<>();
		if(personalOAuthToken != null && !personalOAuthToken.isEmpty()) {
			authMap.put("Authorization: Bearer", personalOAuthToken);
		} else if(privateToken != null && !privateToken.isEmpty()) {
			authMap.put("PRIVATE-TOKEN", privateToken);
		}
		headersMap.add(authMap);
		
		String responseData = AbstractHttpHelper.getRequest(headersMap, url, keyStore, keyStorePass, keyPass);
		Gson gson = new Gson();
        return gson.fromJson(responseData, List.class);
	}
	
	/**
	 * From a job map - parse out id 
	 * @param jobMap
	 * @return
	 */
	public static String getJobIdFromJobMap(Map<String, Object> jobMap) {
		return (String) jobMap.get("id");
	}
	
	/**
	 * 
	 * @param host
	 * @param gitProjectId
	 * @param jobId
	 * @param personalOAuthToken
	 * @param privateToken
	 * @param useApplicationCert
	 * @param saveFilePath
	 * @param saveFileName
	 * @return
	 */
	public static File pullJobArtifact(String host, String gitProjectId, String jobId, String personalOAuthToken, String privateToken, boolean useApplicationCert, String saveFilePath, String saveFileName) {
		String url = host;
		if(url.endsWith("/")) {
			url += "api/v4/projects/"+gitProjectId+"/jobs/"+jobId+"/artifacts";
		} else {
			url += "/api/v4/projects/"+gitProjectId+"/jobs/"+jobId+"/artifacts";
		}
		
		String keyStore = null;
		String keyStorePass = null;
		String keyPass = null;
		if(useApplicationCert) {
			keyStore = DIHelper.getInstance().getProperty(Constants.SCHEDULER_KEYSTORE);
			keyStorePass = DIHelper.getInstance().getProperty(Constants.SCHEDULER_KEYSTORE_PASSWORD);
			keyPass = DIHelper.getInstance().getProperty(Constants.SCHEDULER_CERTIFICATE_PASSWORD);
		}
		
		List<Map<String, String>> headersMap = new ArrayList<>();
		Map<String, String> authMap = new HashMap<>();
		if(personalOAuthToken != null && !personalOAuthToken.isEmpty()) {
			authMap.put("Authorization: Bearer", personalOAuthToken);
		} else if(privateToken != null && !privateToken.isEmpty()) {
			authMap.put("PRIVATE-TOKEN", privateToken);
		}
		headersMap.add(authMap);

		if(saveFileName == null || saveFileName.isEmpty()) {
			saveFileName = "artifact.zip";
		}
		
		File artifact = AbstractHttpHelper.getRequestFileDownload(headersMap, url, keyStore, keyStorePass, keyPass, saveFilePath, saveFileName);
		return artifact;
	}
	
	
	
	
	
	
	
	
	
	
	
	private GitlabUtility() {
		
	}
	
}
