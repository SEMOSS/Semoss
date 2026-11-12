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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

class UserUnitTests {

	private static User nativeUser() {
		return userWith(AuthProvider.NATIVE, "jdoe");
	}

	private static User userWith(AuthProvider type, String id) {
		AccessToken token = new AccessToken();
		token.setProvider(type);
		token.setId(id);
		User user = new User();
		user.setAccessToken(token);
		return user;
	}

	@Test
	void singleLoginNameIsThePrimaryLoginId() {
		assertEquals("jdoe", User.getSingleLogginName(nativeUser()));
	}

	@Test
	void singleLoginNameIsEmptyWithoutAUser() {
		assertEquals("", User.getSingleLogginName(null));
	}

	@Test
	void singleLoginNameIsEmptyOnceTheLastLoginIsDropped() {
		User user = nativeUser();
		user.getPrimaryLogin();
		user.dropAccessToken(AuthProvider.NATIVE);

		assertEquals("", User.getSingleLogginName(user));
	}

	@Test
	void aUserIsTheSamePersonAsItself() {
		User user = nativeUser();

		assertTrue(user.isSamePerson(user));
	}

	@Test
	void usersSharingALoginAreTheSamePerson() {
		User user = nativeUser();
		user.setAccessToken(userWith(AuthProvider.MICROSOFT, "jdoe@corp.com").getAccessToken(AuthProvider.MICROSOFT));

		assertTrue(user.isSamePerson(nativeUser()));
		assertTrue(nativeUser().isSamePerson(user));
	}

	@Test
	void theSameIdUnderAnotherLoginTypeIsAnotherPerson() {
		assertFalse(nativeUser().isSamePerson(userWith(AuthProvider.MICROSOFT, "jdoe")));
		assertFalse(nativeUser().isSamePerson(userWith(AuthProvider.NATIVE, "asmith")));
	}

	@Test
	void noUserIsNotTheSamePerson() {
		assertFalse(nativeUser().isSamePerson(null));
	}
}
