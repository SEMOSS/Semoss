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
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.MockedStatic;

import prerna.auth.User;
import prerna.collaboration.WorkMainTwin;
import prerna.om.Insight;
import prerna.sablecc2.om.NounStore;

/**
 * Checks what the save of a person's own Digital Twin hands on: a key left out stays, one passed empty is cleared.
 */
class WorkSaveMainTwinReactorUnitTests {

	private User user;
	private MockedStatic<WorkMainTwin> twin;

	@BeforeEach
	void setup() {
		user = mock(User.class);
		twin = mockStatic(WorkMainTwin.class);
	}

	@AfterEach
	void cleanup() {
		twin.close();
	}

	private WorkSaveMainTwinReactor reactor(String name, String image) {
		WorkSaveMainTwinReactor reactor = new WorkSaveMainTwinReactor() {
			@Override
			protected User getUser() {
				return user;
			}
		};
		reactor.setInsight(mock(Insight.class));
		NounStore store = new NounStore("save-main-twin");
		if (name != null) {
			store.makeNoun("name").addLiteral(name);
		}
		if (image != null) {
			store.makeNoun("image").addLiteral(image);
		}
		reactor.setNounStore(store);
		return reactor;
	}

	@Test
	void aKeyLeftOutIsLeftAloneAndAnEmptyOneIsCleared() {
		twin.when(() -> WorkMainTwin.save(user, "Ops Twin", null)).thenReturn(Map.of("name", "Ops Twin"));
		twin.when(() -> WorkMainTwin.save(user, null, "aGVsbG8=")).thenReturn(Map.of("name", "A", "image", "cG5n"));
		twin.when(() -> WorkMainTwin.save(user, "", "")).thenReturn(Map.of("name", "Owner's Digital Twin"));

		// only the name: the picture is passed on as not given
		assertEquals(Map.of("name", "Ops Twin"), reactor("Ops Twin", null).execute().getValue());
		// only the picture
		assertEquals(Map.of("name", "A", "image", "cG5n"), reactor(null, "aGVsbG8=").execute().getValue());
		// empty is not left out: it reaches the store as empty, which clears
		assertEquals(Map.of("name", "Owner's Digital Twin"), reactor("", "").execute().getValue());
	}

	@Test
	void aCallWithNeitherIsRefusedByTheStore() {
		IllegalArgumentException refusal = new IllegalArgumentException("Give a name or a picture to save.");
		twin.when(() -> WorkMainTwin.save(user, null, null)).thenThrow(refusal);
		assertSame(refusal, assertThrows(IllegalArgumentException.class, () -> reactor(null, null).execute()));
	}
}
