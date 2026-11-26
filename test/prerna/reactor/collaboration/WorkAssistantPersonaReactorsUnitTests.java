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
package prerna.reactor.collaboration;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.User;
import prerna.collaboration.BrainProfileUtils;
import prerna.collaboration.WorkTwinPersona;
import prerna.om.Insight;
import prerna.sablecc2.om.NounStore;

/**
 * Checks the owner's assistant uses only their enabled persona for writing.
 */
class WorkAssistantPersonaReactorsUnitTests {

	private static final String PERSONA = "How the owner sounds and where they stand.\n\n### Voice\nDirect.";

	private User user;
	private MockedStatic<WorkTwinPersona> persona;
	private MockedStatic<BrainProfileUtils> profile;

	@BeforeEach
	void setup() {
		user = mock(User.class);
		persona = mockStatic(WorkTwinPersona.class);
		profile = mockStatic(BrainProfileUtils.class);
	}

	@AfterEach
	void cleanup() {
		profile.close();
		persona.close();
	}

	private WorkSetAssistantPersonaReactor switchReactor(String enabled) {
		WorkSetAssistantPersonaReactor reactor = new WorkSetAssistantPersonaReactor() {
			@Override
			protected User getUser() {
				return user;
			}
		};
		reactor.setInsight(mock(Insight.class));
		NounStore store = new NounStore("set-assistant-persona");
		if (enabled != null) {
			store.makeNoun("enabled").addLiteral(enabled);
		}
		reactor.setNounStore(store);
		return reactor;
	}

	private BrainGetWritingStyleReactor styleReactor() {
		BrainGetWritingStyleReactor reactor = new BrainGetWritingStyleReactor() {
			@Override
			protected User getUser() {
				return user;
			}
		};
		reactor.setInsight(mock(Insight.class));
		reactor.setNounStore(new NounStore("get-writing-style"));
		return reactor;
	}

	@Test
	void theSignedInPersonSwitchesTheirPersonaForTheirOwnAssistant() {
		persona.when(() -> WorkTwinPersona.setAssistant(user, false)).thenReturn(Map.of("enabled", false));
		persona.when(() -> WorkTwinPersona.setAssistant(user, true)).thenReturn(Map.of("enabled", true));

		assertEquals(Map.of("enabled", false), switchReactor("false").execute().getValue());
		assertEquals(Map.of("enabled", true), switchReactor("true").execute().getValue());
		// a missing switch is never read as on or off
		assertThrows(IllegalArgumentException.class, () -> switchReactor(null).execute());
		persona.verify(() -> WorkTwinPersona.setAssistant(any(), anyBoolean()), times(2));
	}

	@Test
	void writingAsTheOwnerUsesOnlyTheEnabledPersona() {
		persona.when(() -> WorkTwinPersona.forAssistant(user)).thenReturn(PERSONA);

		Map<?, ?> result = (Map<?, ?>) styleReactor().execute().getValue();
		assertEquals(PERSONA, result.get("persona"));
		assertEquals(false, result.get("confirmed"));
		assertEquals("", result.get("summary"));
		assertEquals(List.of(), result.get("examples"));
		assertFalse(result.containsKey("variant"));

		persona.when(() -> WorkTwinPersona.forAssistant(user)).thenReturn(null);
		assertEquals(Map.of("confirmed", false, "audience", "unknown", "summary", "", "examples", List.of()),
				styleReactor().execute().getValue());
		profile.verifyNoInteractions();
	}
}
