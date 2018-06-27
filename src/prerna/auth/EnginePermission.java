package prerna.auth;

public enum EnginePermission {
	
	OWNER (1, "OWNER"),
	EDIT (2, "EDIT"),
	READ_ONLY (3, "READ_ONLY");
	
	private final int id;
	private final String permission;
	
	EnginePermission(int id, String permission) {
		this.id = id;
		this.permission = permission;
	}
	
	public int getId() {
		return this.id;
	}
	
	public String getPermission() {
		return this.permission;
	}
	
	public static EnginePermission getPermissionByValue(String value) {
		EnginePermission ep = EnginePermission.READ_ONLY;
		for(EnginePermission perm : EnginePermission.values()) {
			if(perm.permission.equalsIgnoreCase(value)) {
				ep = perm;
			}
		}
		
		return ep;
	}
	
	public static String getPermissionValueById(String id) {
		EnginePermission ep = EnginePermission.READ_ONLY;
		for(EnginePermission perm : EnginePermission.values()) {
			String permId = perm.id + "";
			if(permId.equalsIgnoreCase(id)) {
				ep = perm;
			}
		}
		
		return ep.getPermission();
	}
}
