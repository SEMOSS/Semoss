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
package prerna.theme;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import org.junit.jupiter.api.Test;
import prerna.auth.User;
import prerna.auth.utils.SecurityAdminUtils;
import prerna.util.JdbcTestDatabase;

class AdminThemeUtilsUnitTests {
	@Test
	void requiresAdminAndPreservesSerializedThemeText() throws Exception {
		try (var db = new JdbcTestDatabase(); var admin = mockStatic(SecurityAdminUtils.class)) {
			User user = mock(User.class);
			assertNull(AdminThemeUtils.getInstance(user));
			verify(db.engine, never()).getConnection();
			admin.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(true);
			var themes = AdminThemeUtils.getInstance(user);
			db.execute("CREATE TABLE ADMIN_THEME (ID VARCHAR, THEME_NAME VARCHAR, THEME_MAP CLOB, IS_ACTIVE BOOLEAN)");
			db.manual();
			String text = "  {\"color\":\"blue\"}  ";
			String id = themes.createAdminTheme("name", text, false);
			assertNotNull(id);
			db.connection.rollback();
			assertEquals(text, db.value("SELECT THEME_MAP FROM ADMIN_THEME"));
			assertTrue(themes.editAdminTheme(id, "new", null, false));
			assertNull(db.value("SELECT THEME_MAP FROM ADMIN_THEME"));
			assertTrue(themes.setAllThemesInactive());
			assertTrue(themes.deleteAdminTheme(id));
			assertTrue(themes.deleteAdminTheme("absent"));
		}
	}

	@Test
	void failedCreateReturnsNullAfterRollback() throws Exception {
		try (var db = new JdbcTestDatabase(); var admin = mockStatic(SecurityAdminUtils.class)) {
			User user = mock(User.class);
			admin.when(() -> SecurityAdminUtils.userIsAdmin(user)).thenReturn(true);
			db.manual();
			assertNull(AdminThemeUtils.getInstance(user).createAdminTheme("n", "{}", false));
			verify(db.connection).rollback();
			verify(db.connection, never()).commit();
		}
	}
}
