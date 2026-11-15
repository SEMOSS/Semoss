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
package prerna.reactor.engine;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.om.Insight;
import prerna.sablecc2.om.NounStore;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Utility;

class EnginePyReactorUnitTests {

	private static final String ENGINE_ID = "python-model";

	private MockedStatic<SecurityEngineUtils> security;
	private MockedStatic<Utility> utility;
	private Insight insight;

	@BeforeEach
	void setUp() {
		this.security = mockStatic(SecurityEngineUtils.class);
		this.utility = mockStatic(Utility.class);
		this.insight = mock(Insight.class);
		when(this.insight.getUser()).thenReturn(mock(User.class));
		this.utility.when(() -> Utility.getEngine(ENGINE_ID)).thenReturn(mock(IEngine.class));
	}

	@AfterEach
	void tearDown() {
		this.security.close();
		this.utility.close();
	}

	@Test
	void anEditorWhoIsNotAnOwnerIsRefused() {
		this.security.when(() -> SecurityEngineUtils.userCanEditEngine(any(), eq(ENGINE_ID))).thenReturn(true);
		this.security.when(() -> SecurityEngineUtils.userIsOwner(any(), eq(ENGINE_ID))).thenReturn(false);

		IllegalArgumentException refused = assertThrows(IllegalArgumentException.class, () -> run());

		assertTrue(refused.getMessage().contains("not an owner"), refused.getMessage());
	}

	@Test
	void anOwnerGetsPastThePermissionCheck() {
		this.security.when(() -> SecurityEngineUtils.userIsOwner(any(), eq(ENGINE_ID))).thenReturn(true);

		IllegalArgumentException notPython = assertThrows(IllegalArgumentException.class, () -> run());

		assertTrue(notPython.getMessage().contains("is not a Python model engine"), notPython.getMessage());
	}

	@Test
	void aSignedOutCallerMustLogIn() {
		when(this.insight.getUser()).thenReturn(null);

		SemossPixelException loginRequired = assertThrows(SemossPixelException.class, () -> run());

		assertTrue(loginRequired.getNoun().getOpType().contains(PixelOperationType.LOGGIN_REQUIRED_ERROR));
	}

	private void run() {
		EnginePyReactor reactor = new EnginePyReactor();
		NounStore nouns = new NounStore("all");
		nouns.makeGenRowStruct("engine").add(new NounMetadata(ENGINE_ID, PixelDataType.CONST_STRING));
		nouns.makeGenRowStruct("code").add(new NounMetadata("print(1)", PixelDataType.CONST_STRING));
		reactor.setNounStore(nouns);
		reactor.setInsight(this.insight);
		reactor.execute();
	}
}
