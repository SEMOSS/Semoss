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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.never;

import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.User;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.collaboration.WorkTwinPersona;
import prerna.om.Insight;
import prerna.sablecc2.om.NounStore;

/**
 * Who may switch a persona on or off for a Digital Twin: only the person who created the twin, whose persona it is,
 * and nobody else is told whether a project is a twin.
 */
class WorkSetTwinPersonaReactorUnitTests {

	private static final String REFUSED = "does not exist or is not yours";

	private User user;
	private MockedStatic<SecurityProjectUtils> security;
	private MockedStatic<WorkTwinPersona> persona;

	@BeforeEach
	void setup() {
		user = mock(User.class);
		security = mockStatic(SecurityProjectUtils.class);
		persona = mockStatic(WorkTwinPersona.class);
	}

	@AfterEach
	void cleanup() {
		persona.close();
		security.close();
	}

	private WorkSetTwinPersonaReactor reactor(String project, String enabled) {
		WorkSetTwinPersonaReactor reactor = new WorkSetTwinPersonaReactor() {
			@Override
			protected User getUser() {
				return user;
			}
		};
		reactor.setInsight(mock(Insight.class));
		NounStore store = new NounStore("set-twin-persona");
		if (project != null) {
			store.makeNoun("project").addLiteral(project);
		}
		if (enabled != null) {
			store.makeNoun("enabled").addLiteral(enabled);
		}
		reactor.setNounStore(store);
		return reactor;
	}

	@Test
	void theTwinsCreatorSwitchesTheirPersonaForIt() {
		security.when(() -> SecurityProjectUtils.isProjectCreator(user, "twin-1")).thenReturn(true);
		security.when(() -> SecurityProjectUtils.isDigitalTwin("twin-1")).thenReturn(true);
		persona.when(() -> WorkTwinPersona.setTwin(user, "twin-1", false))
				.thenReturn(Map.of("project", "twin-1", "enabled", false));
		persona.when(() -> WorkTwinPersona.setTwin(user, "twin-1", true))
				.thenReturn(Map.of("project", "twin-1", "enabled", true));

		assertEquals(Map.of("project", "twin-1", "enabled", false), reactor(" twin-1 ", "false").execute().getValue());
		assertEquals(Map.of("project", "twin-1", "enabled", true), reactor("twin-1", "true").execute().getValue());
	}

	@Test
	void someoneWhoDidNotCreateTheTwinIsRefusedWithoutLearningItIsOne() {
		// an editor of the twin is not its creator: the persona is not theirs to switch
		security.when(() -> SecurityProjectUtils.userCanEditProject(user, "twin-1")).thenReturn(true);
		security.when(() -> SecurityProjectUtils.isProjectCreator(user, "twin-1")).thenReturn(false);
		security.when(() -> SecurityProjectUtils.isDigitalTwin("twin-1")).thenReturn(true);

		RuntimeException refused = assertThrows(IllegalArgumentException.class,
				() -> reactor("twin-1", "false").execute());
		assertTrue(refused.getMessage().contains(REFUSED), refused.getMessage());
		security.verify(() -> SecurityProjectUtils.isDigitalTwin(any()), never());
		persona.verifyNoInteractions();
	}

	@Test
	void aProjectThatIsNotATwinGetsTheSameAnswer() {
		security.when(() -> SecurityProjectUtils.isProjectCreator(user, "plain-app")).thenReturn(true);
		security.when(() -> SecurityProjectUtils.isDigitalTwin("plain-app")).thenReturn(false);

		RuntimeException refused = assertThrows(IllegalArgumentException.class,
				() -> reactor("plain-app", "false").execute());
		assertTrue(refused.getMessage().contains(REFUSED), refused.getMessage());

		// both keys are needed: a missing switch is never read as on or off
		assertThrows(IllegalArgumentException.class, () -> reactor("  ", "false").execute());
		assertThrows(IllegalArgumentException.class, () -> reactor("plain-app", null).execute());
		persona.verifyNoInteractions();
	}
}
