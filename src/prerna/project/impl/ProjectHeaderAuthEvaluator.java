package prerna.project.impl;

import java.io.UnsupportedEncodingException;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;

import org.apache.http.HttpHeaders;

public class ProjectHeaderAuthEvaluator {

	private String projectId;
	private String method = "getAuthorizationHeader";
	private transient String accessKey;
	private transient String secretKey;
	
	public ProjectHeaderAuthEvaluator() {
		
	}

	//TODO: expand on this to allow other login types outside of basics
	
	public Map<String, String> eval() throws UnsupportedEncodingException {
		String concat = this.accessKey + ":" + this.secretKey;
		byte[] encoded = Base64.getEncoder().encode(concat.getBytes("UTF-8"));
		
		Map<String, String> headers = new HashMap<>();
		headers.put(HttpHeaders.AUTHORIZATION, "Basic " + new String(encoded));
		return headers;
	}
	
	public String getProjectId() {
		return projectId;
	}

	public void setProjectId(String projectId) {
		this.projectId = projectId;
	}

	public void setAccessKey(String accessKey) {
		this.accessKey = accessKey;
	}

	public void setSecretKey(String secretKey) {
		this.secretKey = secretKey;
	}
	
}
