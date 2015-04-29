package prerna.auth;

public class EngineAccessRequest {
	private String requestId;
	private String userName;
	private String engineName;
	
	public EngineAccessRequest(String requestId, String userId, String engineName) {
		this.requestId = requestId;
		this.userName = userId;
		this.engineName = engineName;
	}
	
	public String getRequestId() {
		return this.requestId;
	}
	
	public String getUser() {
		return this.userName;
	}
	
	public String getEngineRequested() {
		return this.engineName;
	}	
}
