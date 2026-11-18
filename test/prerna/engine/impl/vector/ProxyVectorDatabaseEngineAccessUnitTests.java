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
package prerna.engine.impl.vector;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.MockedStatic;
import org.mockito.Mockito;

import prerna.auth.AccessPermissionEnum;
import prerna.auth.User;
import prerna.auth.access.EngineGrantee;
import prerna.auth.utils.SecurityEngineAccessUtils;
import prerna.engine.impl.pipeline.PlatformOperation;
import prerna.om.ThreadStore;

class ProxyVectorDatabaseEngineAccessUnitTests {

	private static final String PROXY = "proxy-vector";
	private static final String TARGET = "target-vector";
	private static final EngineGrantee PROXY_ENGINE = new EngineGrantee(PROXY, "VECTOR");

	private MockedStatic<SecurityEngineAccessUtils> access;
	private User user;

	@BeforeEach
	void setup() {
		this.access = Mockito.mockStatic(SecurityEngineAccessUtils.class);
		this.user = mock(User.class);
		ThreadStore.setUser(this.user);
	}

	@AfterEach
	void cleanup() {
		ThreadStore.remove();
		this.access.close();
	}

	@ParameterizedTest
	@EnumSource(value = AccessPermissionEnum.class, names = { "EDIT", "OWNER" })
	void aTargetThatGivesTheProxyEditorLetsItOpenACopy(AccessPermissionEnum granted) {
		grants(granted);

		assertDoesNotThrow(() -> ProxyVectorDatabaseEngine.requireEditorOnTarget(PROXY, TARGET));
	}

	@Test
	void aReadOnlyPermissionIsNotEnoughToCopyTheTargetsSettings() {
		grants(AccessPermissionEnum.READ_ONLY);

		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> ProxyVectorDatabaseEngine.requireEditorOnTarget(PROXY, TARGET));

		assertEquals("Vector database " + TARGET + " does not give proxy vector database " + PROXY
				+ " Editor access, which it needs to open a copy of it", refusal.getMessage());
	}

	@Test
	void aTargetThatGivesTheProxyNothingIsRefused() {
		assertThrows(IllegalArgumentException.class,
				() -> ProxyVectorDatabaseEngine.requireEditorOnTarget(PROXY, TARGET));
	}

	@Test
	void theTargetsOwnerOpensTheCopyWithoutAPermission() {
		this.access.when(() -> SecurityEngineAccessUtils.getUserPermission(this.user, TARGET, false))
				.thenReturn(AccessPermissionEnum.OWNER);

		assertDoesNotThrow(() -> ProxyVectorDatabaseEngine.requireEditorOnTarget(PROXY, TARGET));
	}

	@Test
	void anEditorOfTheTargetStillNeedsThePermission() {
		this.access.when(() -> SecurityEngineAccessUtils.getUserPermission(this.user, TARGET, false))
				.thenReturn(AccessPermissionEnum.EDIT);

		assertThrows(IllegalArgumentException.class,
				() -> ProxyVectorDatabaseEngine.requireEditorOnTarget(PROXY, TARGET));
	}

	@Test
	void thePlatformsOwnWorkOpensTheCopy() {
		ThreadStore.remove();

		try (PlatformOperation operation = PlatformOperation.begin("test")) {
			assertDoesNotThrow(() -> ProxyVectorDatabaseEngine.requireEditorOnTarget(PROXY, TARGET));
		}
	}

	private void grants(AccessPermissionEnum level) {
		this.access.when(() -> SecurityEngineAccessUtils.getGranteePermission(PROXY_ENGINE, TARGET))
				.thenReturn(level);
	}
}
