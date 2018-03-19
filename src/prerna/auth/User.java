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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;

public class User {
	
	// login types
	public static enum LOGIN_TYPES {GOOGLE, FACEBOOK, TWITTER, CAC, EMAIL, AD, ANONYMOUS};
	
	// user metadata
	private String userId;
	private String name;
	private LOGIN_TYPES loginType;
	private String email;
	private String pictureUrl= "";
	private boolean admin = false;
	// this is used as a catch all for specific objects we would
	// want to store based on the specific login type
	private Map<String, Object> additionalData;
	
	// permissions for user
	private HashMap<String, HashMap<String, Boolean>> enginePermissions = new HashMap<String, HashMap<String, Boolean>>();
	private Hashtable<String, String> userParamPreferences = new Hashtable<String, String>();
	private Hashtable<String, Object> customProps = new Hashtable<String, Object>();
	
	public User(String id, String name, LOGIN_TYPES type, String email) {
		this.userId = id;
		this.name = name;
		this.loginType = type;
		this.email = email;
		this.additionalData = new HashMap<String, Object>();
	}
	
	public User(String userId, String name, LOGIN_TYPES type, String email, String picture) {
		this.userId = userId;
		this.name = name;
		this.loginType = type;
		this.email = email;
		this.pictureUrl = picture;
		this.additionalData = new HashMap<String, Object>();
	}
	
	public String getId() {
		return this.userId;
	}
	
	public String getName() {
		return this.name;
	}
	
	public String getLoginType() {
		return this.loginType.toString();
	}
	
	public String getEmail() {
		return this.email;
	}
	
	public String getPicture() {
		return this.pictureUrl;
	}
	
	public Object getAdditionalData(String key) {
		return this.additionalData.get(key);
	}
	
	public void setAdditionalData(String key, Object obj) {
		this.additionalData.put(key, obj);
	}
	
	public String[] getPermissionsForEngine(String engine) {
		HashMap<String, Boolean> map = this.enginePermissions.get(engine);
		ArrayList<String> permissions = new ArrayList<String>();
		for(String s : map.keySet()) {
			if(map.get(s)) {
				permissions.add(s);
			}
		}
		
		return (String[]) permissions.toArray();
	}
	
	public void addPermissionsForEngine(String engine, HashMap<String, Boolean> permissions) {
		this.enginePermissions.put(engine, permissions);
	}
	
	public Hashtable<String, String> getParamPreferences() {
		return this.userParamPreferences;
	}
	
	public void setParamPreferences(Hashtable<String, String> preferences) {
		this.userParamPreferences = preferences;
	}
	
	public Object getCustomProp(String key) {
		return this.customProps.get(key);
	}
	
	public void setCustomProp(String key, Object value) {
		this.customProps.put(key, value);
	}
	
	public void setAdmin(boolean isAdmin) {
		this.admin = isAdmin;
	}
	
	public boolean isAdmin() {
		return this.admin;
	}
}
