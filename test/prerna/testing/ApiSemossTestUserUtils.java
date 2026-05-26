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
package prerna.testing;

import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.apache.commons.io.FileUtils;

import prerna.auth.AccessToken;
import prerna.auth.AuthProvider;
import prerna.auth.User;

public class ApiSemossTestUserUtils {
	
	private static User USER = null; 
	
	public static User getUser() {
		return USER;
	}

	public static void setDefaultTestUser() {
		setUser(ApiTestsSemossConstants.USER_NAME, ApiTestsSemossConstants.USER_EMAIL);
	}
	
	public static void addAndSetNewNativeUser(String userName, String email, boolean isAdmin) {
		createUser(userName, email, isAdmin);
		setUser(userName, email);
	}
	
	private static void createUser(String userName, String email, boolean isAdmin) {
		try {
			ApiSemossTestEngineUtils.createUser(userName, email, AuthProvider.NATIVE.toString(), isAdmin);
		} catch (Exception e) {
			System.out.println("Could not create User");
			fail(e.toString());
		}
	}
	
	public static void setUser(String userName, String email) {
		USER = new User();
		AccessToken at = new AccessToken();
		at.setProvider(AuthProvider.NATIVE);
		at.setId(userName);
		at.setEmail(email);
		USER.setAccessToken(at);
		USER.setPrimaryLogin(AuthProvider.NATIVE);
		ApiSemossTestInsightUtils.getInsight().setUser(USER);
	}
	
	public static void clearUserDirectory() throws IOException {
		Path p = Paths.get(ApiTestsSemossConstants.TEST_BASE_DIRECTORY, "user");
		if (Files.exists(p)) {
			FileUtils.cleanDirectory(p.toFile());
		}
	}
	
	public static Path getAssetsPath() {
		String id = USER.getAssetProjectId(USER.getPrimaryLogin());
		String assetId = "Asset__" + id;
		Path p = Paths.get(ApiTestsSemossConstants.TEST_BASE_DIRECTORY, "user", assetId, "app_root", "version", "assets");
		return p;
	}
	
}
