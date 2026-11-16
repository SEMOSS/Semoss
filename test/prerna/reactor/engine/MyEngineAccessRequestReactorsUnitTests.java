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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.auth.utils.AbstractReactorPermissionUnitTestsSetup;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.sablecc2.om.nounmeta.NounMetadata;

/**
 * A user's own access request on an engine: reading the one that waits,
 * and withdrawing it. Each test runs the reactors against the security database
 * of the tests, with the requester's request made as the engine page makes it.
 */
class MyEngineAccessRequestReactorsUnitTests extends AbstractReactorPermissionUnitTestsSetup {

	private static final int EDIT_LEVEL = 2;

	private Map<?, ?> waitingRequestOf(User user) {
		NounMetadata result = run(new GetMyEngineAccessRequestReactor(), user, Map.of("engine", ENGINE));
		return (Map<?, ?>) result.getValue();
	}

	@Test
	void theRequesterSeesTheRequestTheyMade() {
		theRequesterAsksForTheEngine();

		Map<?, ?> request = waitingRequestOf(this.requester);

		assertNotNull(request.get("id"));
		assertEquals("READ_ONLY", request.get("permission"));
		assertEquals("to read it", request.get("comment"));
		assertNotNull(request.get("requestedAt"));
	}

	@Test
	void someoneElseSeesNoRequest() {
		theRequesterAsksForTheEngine();

		assertNull(waitingRequestOf(anEditorWhoIsNotAnAdmin()));
	}

	@Test
	void askingAgainReplacesTheRequest() {
		theRequesterAsksForTheEngine();
		SecurityEngineUtils.setUserAccessRequest(REQUESTER_ID, AuthProvider.NATIVE.name(), ENGINE, "to edit it", EDIT_LEVEL,
				this.requester);

		Map<?, ?> request = waitingRequestOf(this.requester);

		assertEquals("EDIT", request.get("permission"));
		assertEquals("to edit it", request.get("comment"));
	}

	@Test
	void theRequesterWithdrawsTheirRequestAndTheOwnersNoLongerSeeIt() {
		theRequesterAsksForTheEngine();

		NounMetadata result = run(new WithdrawMyEngineAccessRequestReactor(), this.requester, Map.of("engine", ENGINE));

		assertEquals(true, result.getValue());
		assertNull(waitingRequestOf(this.requester));
		assertTrue(SecurityEngineUtils.getUserAccessRequestsByEngine(ENGINE).isEmpty());
	}

	@Test
	void withdrawingWithNoRequestWaitingIsRefused() {
		IllegalArgumentException refusal = assertThrows(IllegalArgumentException.class,
				() -> run(new WithdrawMyEngineAccessRequestReactor(), this.requester, Map.of("engine", ENGINE)));

		assertEquals("You have no access request waiting on this engine", refusal.getMessage());
	}

	@Test
	void someoneElseCannotWithdrawTheRequestersRequest() {
		theRequesterAsksForTheEngine();

		assertThrows(IllegalArgumentException.class,
				() -> run(new WithdrawMyEngineAccessRequestReactor(), anEditorWhoIsNotAnAdmin(), Map.of("engine", ENGINE)));

		assertEquals("READ_ONLY", waitingRequestOf(this.requester).get("permission"));
	}

}
