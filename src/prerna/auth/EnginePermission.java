package prerna.auth;

public enum EnginePermission {
	EXPLORE_NODES ("Explore Nodes", "ExplorePermission"),
	TRAVERSE ("Traverse To/From Nodes", "TraversePermission"),
	CREATE_INSIGHT ("Create Insights", "CreateInsightPermission"),
	COPY_INSIGHT ("Copy Insights", "CopyInsightPermission"),
	EDIT_INSIGHT ("Edit Insights", "EditInsightPermission"),
	DELETE_INSIGHT ("Delete Insights", "DeleteInsightPermission"),
	MODIFY_DATA ("Modify Data", "ModifyDataPermission");
	
	private final String permissionName;
	private final String propertyName;
	
	EnginePermission(String permissionName, String propertyName) {
		this.permissionName = permissionName;
		this.propertyName = propertyName;
	}
	
	public String getPermissionName() {
		return this.permissionName;
	}
	
	public String getPropertyName() {
		return this.propertyName;
	}
	
	public static String getPropertyNameByPermissionName(String permission) {
		String propName = "";
		for(EnginePermission ep : values()) {
			if(ep.getPermissionName().equals(permission)) {
				propName = ep.getPropertyName();
				break;
			}
		}
		
		return propName;
	}
}
