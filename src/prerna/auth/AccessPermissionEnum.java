package prerna.auth;

import java.util.HashMap;
import java.util.Map;

public enum AccessPermissionEnum {
	
	OWNER (1, "OWNER"),
	EDIT (2, "EDIT"),
	READ_ONLY (3, "READ_ONLY");
	
	private final int id;
	private final String permission;
	
	AccessPermissionEnum(int id, String permission) {
		this.id = id;
		this.permission = permission;
	}
	
	public int getId() {
		return this.id;
	}
	
	public String getPermission() {
		return this.permission;
	}
	
	/**
	 * Determine if the permission integer means the user
	 * can modify the database
	 * @param permission
	 * @return
	 */
	public static boolean isEditor(int permission) {
		if(permission == 1 || permission == 2) {
			return true;
		}
		return false;
	}
	
	/**
	 * Determine if the permission integer means the user
	 * can is the database owner
	 * @param permission
	 * @return
	 */
	public static boolean isOwner(int permission) {
		if(permission == 1) {
			return true;
		}
		return false;
	}
	
	public static AccessPermissionEnum getPermissionByValue(String value) {
		AccessPermissionEnum ep = AccessPermissionEnum.READ_ONLY;
		for(AccessPermissionEnum perm : AccessPermissionEnum.values()) {
			if(perm.permission.equalsIgnoreCase(value)) {
				ep = perm;
			}
		}
		
		return ep;
	}
	
	public static String getPermissionValueById(String id) {
		AccessPermissionEnum ep = AccessPermissionEnum.READ_ONLY;
		for(AccessPermissionEnum perm : AccessPermissionEnum.values()) {
			String permId = perm.id + "";
			if(permId.equalsIgnoreCase(id)) {
				ep = perm;
				break;
			}
		}
		
		return ep.getPermission();
	}
	
	public static String getPermissionValueById(int id) {
		AccessPermissionEnum ep = AccessPermissionEnum.READ_ONLY;
		for(AccessPermissionEnum perm : AccessPermissionEnum.values()) {
			if(perm.id == id) {
				ep = perm;
				break;
			}
		}
		
		return ep.getPermission();
	}
	
	public static int getIdByPermission(String id) {
		AccessPermissionEnum ep = AccessPermissionEnum.READ_ONLY;
		for(AccessPermissionEnum perm : AccessPermissionEnum.values()) {
			if(perm.permission.equalsIgnoreCase(id)) {
				ep = perm;
				break;
			}
		}
		
		return ep.getId();
	}
	
	public static Map<Integer, String> flushEnumInteger() {
		AccessPermissionEnum[] values = AccessPermissionEnum.values();
		Map<Integer, String> flushed = new HashMap<>(values.length);
		for(AccessPermissionEnum e : values) {
			flushed.put(e.id, e.permission);
		}
		
		return flushed;
	}
	
	public static Map<String, Integer> flushEnumString() {
		AccessPermissionEnum[] values = AccessPermissionEnum.values();
		Map<String, Integer> flushed = new HashMap<>(values.length);
		for(AccessPermissionEnum e : values) {
			flushed.put(e.permission, e.id);
		}
		
		return flushed;
	}
}
