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
package prerna.auth.utils;

import org.junit.jupiter.api.function.Executable;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.User;

/**
 * Lets a test outside this package run work for a user the security database
 * records as an admin, without a security database.
 */
public final class StoredAdmin {

	private StoredAdmin() {
	}

	/**
	 * Runs work on this thread while the security database's answer for one user
	 * is that the user is an admin. Everything else about
	 * {@link SecurityAdminUtils} is as it is on a server.
	 *
	 * @param admin the user recorded as an admin
	 * @param work  what to run
	 * @throws Throwable what the work threw
	 */
	public static void run(User admin, Executable work) throws Throwable {
		try (MockedStatic<SecurityAdminUtils> stored = Mockito.mockStatic(SecurityAdminUtils.class,
				Mockito.CALLS_REAL_METHODS)) {
			stored.when(() -> SecurityAdminUtils.storedAsAdmin(admin)).thenReturn(true);
			work.execute();
		}
	}
}
