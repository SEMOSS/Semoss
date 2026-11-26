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
import prerna.collaboration.WorkTwinIndex;
import prerna.om.Insight;
import prerna.sablecc2.om.NounStore;

/**
 * Who may bring a Digital Twin's index up to date: only someone who can change
 * the twin, and nobody else is told whether a project is one.
 */
class WorkIndexTwinFilesReactorUnitTests {

	private static final String REFUSED = "does not exist or is not yours to change";

	private User user;
	private MockedStatic<SecurityProjectUtils> security;
	private MockedStatic<WorkTwinIndex> index;

	@BeforeEach
	void setup() {
		user = mock(User.class);
		security = mockStatic(SecurityProjectUtils.class);
		index = mockStatic(WorkTwinIndex.class);
	}

	@AfterEach
	void cleanup() {
		index.close();
		security.close();
	}

	private WorkIndexTwinFilesReactor reactor(String project) {
		WorkIndexTwinFilesReactor reactor = new WorkIndexTwinFilesReactor() {
			@Override
			protected User getUser() {
				return user;
			}
		};
		reactor.setInsight(mock(Insight.class));
		NounStore store = new NounStore("index-twin");
		if (project != null) {
			store.makeNoun("project").addLiteral(project);
		}
		reactor.setNounStore(store);
		return reactor;
	}

	@Test
	void someoneWhoCanChangeTheTwinGetsThePassesAnswer() {
		security.when(() -> SecurityProjectUtils.userCanEditProject(user, "twin-1")).thenReturn(true);
		security.when(() -> SecurityProjectUtils.isDigitalTwin("twin-1")).thenReturn(true);
		index.when(() -> WorkTwinIndex.sync(user, "twin-1")).thenReturn(Map.of("described", 3, "remaining", 2));

		assertEquals(Map.of("described", 3, "remaining", 2), reactor(" twin-1 ").execute().getValue());
	}

	@Test
	void someoneItIsOnlySharedWithIsRefusedWithoutLearningItIsATwin() {
		security.when(() -> SecurityProjectUtils.userCanEditProject(user, "twin-1")).thenReturn(false);
		security.when(() -> SecurityProjectUtils.isDigitalTwin("twin-1")).thenReturn(true);

		RuntimeException refused = assertThrows(IllegalArgumentException.class, () -> reactor("twin-1").execute());
		assertTrue(refused.getMessage().contains(REFUSED), refused.getMessage());
		security.verify(() -> SecurityProjectUtils.isDigitalTwin(any()), never());
		index.verifyNoInteractions();
	}

	@Test
	void aProjectThatIsNotATwinGetsTheSameAnswerAndNoIndex() {
		security.when(() -> SecurityProjectUtils.userCanEditProject(user, "plain-app")).thenReturn(true);
		security.when(() -> SecurityProjectUtils.isDigitalTwin("plain-app")).thenReturn(false);

		RuntimeException refused = assertThrows(IllegalArgumentException.class, () -> reactor("plain-app").execute());
		assertTrue(refused.getMessage().contains(REFUSED), refused.getMessage());
		index.verifyNoInteractions();

		assertThrows(IllegalArgumentException.class, () -> reactor("  ").execute());
		index.verifyNoInteractions();
	}
}
