package prerna.util.git.gitlab;

import java.io.File;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.snowflake.client.jdbc.internal.google.gson.Gson;
import prerna.security.AbstractHttpHelper;
import prerna.util.Constants;
import prerna.util.Utility;

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
			keyStore = Utility.getDIHelperProperty(Constants.SCHEDULER_KEYSTORE);
			keyStorePass = Utility.getDIHelperProperty(Constants.SCHEDULER_KEYSTORE_PASSWORD);
			keyPass = Utility.getDIHelperProperty(Constants.SCHEDULER_CERTIFICATE_PASSWORD);
		}
		
		Map<String, String> headersMap = new HashMap<>();
		if(personalOAuthToken != null && !personalOAuthToken.isEmpty()) {
			headersMap.put("Authorization: Bearer", personalOAuthToken);
		} else if(privateToken != null && !privateToken.isEmpty()) {
			headersMap.put("PRIVATE-TOKEN", privateToken);
		}
		
		String responseData = AbstractHttpHelper.getRequest(url, headersMap, keyStore, keyStorePass, keyPass);
		Gson gson = new Gson();
        return gson.fromJson(responseData, List.class);
	}
	
	/**
	 * 
	 * @param host
	 * @param gitProjectId
	 * @param personalOAuthToken
	 * @param privateToken
	 * @param useApplicationCert
	 * @return
	 */
	public static List<Map<String, Object>> getGitlabBranches(String host, String gitProjectId, String personalOAuthToken, String privateToken, boolean useApplicationCert) {
		String url = host;
		if(url.endsWith("/")) {
			url += "api/v4/projects/"+gitProjectId+"/repository/branches";
		} else {
			url += "/api/v4/projects/"+gitProjectId+"/repository/branches";
		}
		
		String keyStore = null;
		String keyStorePass = null;
		String keyPass = null;
		if(useApplicationCert) {
			keyStore = Utility.getDIHelperProperty(Constants.SCHEDULER_KEYSTORE);
			keyStorePass = Utility.getDIHelperProperty(Constants.SCHEDULER_KEYSTORE_PASSWORD);
			keyPass = Utility.getDIHelperProperty(Constants.SCHEDULER_CERTIFICATE_PASSWORD);
		}
		
		Map<String, String> headersMap = new HashMap<>();
		if(personalOAuthToken != null && !personalOAuthToken.isEmpty()) {
			headersMap.put("Authorization: Bearer", personalOAuthToken);
		} else if(privateToken != null && !privateToken.isEmpty()) {
			headersMap.put("PRIVATE-TOKEN", privateToken);
		}
		
		String responseData = AbstractHttpHelper.getRequest(url, headersMap, keyStore, keyStorePass, keyPass);
		Gson gson = new Gson();
        return gson.fromJson(responseData, List.class);
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
			keyStore = Utility.getDIHelperProperty(Constants.SCHEDULER_KEYSTORE);
			keyStorePass = Utility.getDIHelperProperty(Constants.SCHEDULER_KEYSTORE_PASSWORD);
			keyPass = Utility.getDIHelperProperty(Constants.SCHEDULER_CERTIFICATE_PASSWORD);
		}
		
		Map<String, String> headersMap = new HashMap<>();
		if(personalOAuthToken != null && !personalOAuthToken.isEmpty()) {
			headersMap.put("Authorization: Bearer", personalOAuthToken);
		} else if(privateToken != null && !privateToken.isEmpty()) {
			headersMap.put("PRIVATE-TOKEN", privateToken);
		}

		if(saveFileName == null || saveFileName.isEmpty()) {
			saveFileName = "artifact.zip";
		}
		
		File artifact = AbstractHttpHelper.getRequestFileDownload(url, headersMap, keyStore, keyStorePass, keyPass, saveFilePath, saveFileName);
		return artifact;
	}
	
	/**
	 * 
	 * @param host
	 * @param gitProjectId
	 * @param branch
	 * @param jobName
	 * @param personalOAuthToken
	 * @param privateToken
	 * @param useApplicationCert
	 * @param saveFilePath
	 * @param saveFileName
	 * @return
	 */
	public static File pullLastSuccessfulJobArtifact(String host, String gitProjectId, String branch, String jobName, String personalOAuthToken, String privateToken, boolean useApplicationCert, String saveFilePath, String saveFileName) {
		if(branch == null || (branch=branch.trim()).isEmpty()) {
			throw new NullPointerException("Must provide the branch name");
		}
		if(jobName == null || (jobName=jobName.trim()).isEmpty()) {
			throw new NullPointerException("Must provide the job name");
		}
		String url = host;
		if(url.endsWith("/")) {
			url += "api/v4/projects/"+gitProjectId+"/jobs/artifacts/"+branch+"/download?job="+jobName;
		} else {
			url += "/api/v4/projects/"+gitProjectId+"/jobs/artifacts/"+branch+"/download?job="+jobName;
		}
		
		String keyStore = null;
		String keyStorePass = null;
		String keyPass = null;
		if(useApplicationCert) {
			keyStore = Utility.getDIHelperProperty(Constants.SCHEDULER_KEYSTORE);
			keyStorePass = Utility.getDIHelperProperty(Constants.SCHEDULER_KEYSTORE_PASSWORD);
			keyPass = Utility.getDIHelperProperty(Constants.SCHEDULER_CERTIFICATE_PASSWORD);
		}
		
		Map<String, String> headersMap = new HashMap<>();
		if(personalOAuthToken != null && !personalOAuthToken.isEmpty()) {
			headersMap.put("Authorization: Bearer", personalOAuthToken);
		} else if(privateToken != null && !privateToken.isEmpty()) {
			headersMap.put("PRIVATE-TOKEN", privateToken);
		}

		if(saveFileName == null || saveFileName.isEmpty()) {
			saveFileName = "artifact.zip";
		}
		
		File artifact = AbstractHttpHelper.getRequestFileDownload(url, headersMap, keyStore, keyStorePass, keyPass, saveFilePath, saveFileName);
		return artifact;
	}
	
	
	
	
	
	
	
	
	
	
	/**
	 * 
	 */
	private GitlabUtility() {
		
	}
	
}
