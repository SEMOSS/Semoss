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
package prerna.collaboration;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

/** Who the newest message of a thread is addressed to, as the classifier's FYI rule reads it. */
class BrainThreadClassifierUnitTests {

	private static final BrainThreadClassifier.Self OWNER = new BrainThreadClassifier.Self("p-owner", "Owner",
			Set.of("owner@test.com"));
	private static final Map<String, Object> NOBODY = Map.of("to", List.of(), "cc", List.of());

	@Test
	void aTeamsMessageIsAddressedToTheOwnerThoughItsLiveReadCarriesNoRecipients() {
		assertEquals("to", BrainThreadClassifier.recipientRole(OWNER, NOBODY, "teams"));
	}

	@Test
	void mailNamesTheOwnerOnToOrCcOrNotAtAll() {
		List<Map<String, Object>> owner = List.of(Map.of("address", "owner@test.com"));
		assertEquals("to", BrainThreadClassifier.recipientRole(OWNER, Map.of("to", owner), "email"));
		assertEquals("cc", BrainThreadClassifier.recipientRole(OWNER, Map.of("to", List.of(), "cc", owner), "email"));
		assertEquals("none", BrainThreadClassifier.recipientRole(OWNER, NOBODY, "email"));
	}

	@Test
	void aColleagueWithTheOwnersNameIsNotTheOwner() {
		List<Map<String, Object>> namesake = List.of(Map.of("name", "Owner", "address", "owner@elsewhere.com"));
		assertEquals("none",
				BrainThreadClassifier.recipientRole(OWNER, Map.of("to", namesake, "cc", namesake), "email"));
	}
}
