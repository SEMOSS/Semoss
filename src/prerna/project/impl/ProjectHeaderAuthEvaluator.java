package prerna.project.impl;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
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

	// TODO: expand on this to allow other login types outside of basics

	// public Map<String, String> eval() throws UnsupportedEncodingException {
	// 	String concat = this.accessKey + ":" + this.secretKey;
	// 	byte[] encoded = Base64.getEncoder().encode(concat.getBytes(StandardCharsets.UTF_8));

	// 	Map<String, String> headers = new HashMap<>();
	// 	headers.put(HttpHeaders.AUTHORIZATION, "Basic " + new String(encoded));
	// 	return headers;
	// }
	public Map<String, String> eval() throws UnsupportedEncodingException {
		char[] accKy = this.accessKey.toCharArray();
    	char[] secKy = this.secretKey.toCharArray();
		char[] concat = new char[accKy.length + 1 + secKy.length];
		
		System.arraycopy(accKy, 0, concat, 0, accKy.length);
		concat[accKy.length] = ':';
		System.arraycopy(secKy, 0, concat, accKy.length + 1, secKy.length);
		
		byte[] encoded = Base64.getEncoder()
			.encode(new String(concat).getBytes(StandardCharsets.UTF_8));
		
		Arrays.fill(accKy, '\0');
	    Arrays.fill(secKy, '\0');
	    Arrays.fill(concat, '\0');
	    
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
