package prerna.auth;

public class EngineAccessRequest {
	private String requestId;
	private String userName;
	private String engineName;
	private String[] permissions;
	
	public EngineAccessRequest(String requestId, String userId, String engineName, String[] permissions) {
		this.requestId = requestId;
		this.userName = userId;
		this.engineName = engineName;
		this.permissions = permissions;
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
	
	public String[] getPermissionsRequested() {
		return this.permissions;
	}
}
