/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * If your use of this software does not include any GPLv2 components:
 * 	Licensed under the Apache License, Version 2.0 (the "License");
 * 	you may not use this file except in compliance with the License.
 * 	You may obtain a copy of the License at
 *
 * 	  http://www.apache.org/licenses/LICENSE-2.0
 *
 * 	Unless required by applicable law or agreed to in writing, software
 * 	distributed under the License is distributed on an "AS IS" BASIS,
 * 	WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * 	See the License for the specific language governing permissions and
 * 	limitations under the License.
 * ----------------------------------------------------------------------------
 * If your use of this software includes any GPLv2 components:
 * 	This program is free software; you can redistribute it and/or
 * 	modify it under the terms of the GNU General Public License
 * 	as published by the Free Software Foundation; either version 2
 * 	of the License, or (at your option) any later version.
 *
 * 	This program is distributed in the hope that it will be useful,
 * 	but WITHOUT ANY WARRANTY; without even the implied warranty of
 * 	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * 	GNU General Public License for more details.
 *******************************************************************************/
package prerna.auth;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import prerna.util.ValueUtils;

public enum AccessPermissionEnum {
	
	OWNER (1, "OWNER"),
	EDIT (2, "EDIT"),
	READ_ONLY (3, "READ_ONLY");
	
	/** The key that holds the level on each row of a bulk permission request */
	private static final String ROW_PERMISSION_KEY = "permission";

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
	
	/**
	 * Returns the name of the level a caller sends, such as EDIT for "edit",
	 * ignoring case and surrounding spaces. Unlike
	 * {@link #getIdByPermission(String)}, a missing or unknown level is refused
	 * rather than read as READ_ONLY, so a typo cannot demote anyone.
	 *
	 * @param permission the level's name
	 * @return OWNER, EDIT or READ_ONLY
	 * @throws IllegalArgumentException when the level is missing or is not one of
	 *                                  them
	 */
	public static String require(String permission) {
		if (permission != null) {
			String name = permission.trim();
			for (AccessPermissionEnum perm : AccessPermissionEnum.values()) {
				if (perm.permission.equalsIgnoreCase(name)) {
					return perm.permission;
				}
			}
		}
		throw new IllegalArgumentException(
				"The permission must be OWNER, EDIT or READ_ONLY. Received value = " + permission);
	}

	/**
	 * Checks the level on each row of a bulk request, as {@link #require(String)}
	 * does, and writes the level's name back to the row's {@code permission}.
	 *
	 * @param <M>  the row type
	 * @param rows the rows, or null when the request sends none
	 * @return the rows
	 * @throws IllegalArgumentException when a row is missing or has no valid level
	 */
	public static <M extends Map<String, ? super String>> List<M> requireEach(List<M> rows) {
		if (rows == null) {
			return null;
		}
		for (M row : rows) {
			if (row == null) {
				throw new IllegalArgumentException("Each row must be an object with a permission");
			}
			row.put(ROW_PERMISSION_KEY, require(ValueUtils.toStringOrNull(row.get(ROW_PERMISSION_KEY))));
		}
		return rows;
	}

	/**
	 * Returns the level with this id: 1 for OWNER, 2 for EDIT or 3 for READ_ONLY.
	 * Unlike {@link #getPermissionValueById(int)}, an unknown id is refused rather
	 * than read as READ_ONLY.
	 *
	 * @param id the level's id
	 * @return the level
	 * @throws IllegalArgumentException when the id is not a level
	 */
	public static AccessPermissionEnum requireById(int id) {
		for (AccessPermissionEnum perm : AccessPermissionEnum.values()) {
			if (perm.id == id) {
				return perm;
			}
		}
		throw new IllegalArgumentException(
				"The permission must be 1 (OWNER), 2 (EDIT) or 3 (READ_ONLY). Received value = " + id);
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
