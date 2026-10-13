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
/*******************************************************************************
 * Copyright 2015 Defense Health Agency (DHA)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *******************************************************************************/
package prerna.reactor.automation.run;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;

import org.junit.jupiter.api.Test;

import prerna.auth.User;
import prerna.reactor.automation.AutomationConstants;
import prerna.reactor.automation.definition.AutomationDefinitionValidator;
import prerna.reactor.scheduler.SchedulerUserAccess;

class TriggerAutomationReactorUnitTests {

	@Test
	void synchronousPrincipalAccessIsPersistedOnlyWhenValidatedDefinitionHasDurableItems() {
		User user = mock(User.class);
		AutomationDefinitionValidator.ValidatedDefinition ordinary = definition(false);
		AutomationDefinitionValidator.ValidatedDefinition durable = definition(true);
		try (var access = mockStatic(SchedulerUserAccess.class)) {
			access.when(() -> SchedulerUserAccess.encode(user)).thenReturn("encoded-principal");

			assertNull(TriggerAutomationReactor.principalAccess(user, false, ordinary));
			access.verifyNoInteractions();
			assertEquals("encoded-principal", TriggerAutomationReactor.principalAccess(user, false, durable));
			access.verify(() -> SchedulerUserAccess.encode(user));
		}
	}

	@Test
	void asynchronousPrincipalAccessRemainsPersistedWithoutDurableItems() {
		User user = mock(User.class);
		try (var access = mockStatic(SchedulerUserAccess.class)) {
			access.when(() -> SchedulerUserAccess.encode(user)).thenReturn("encoded-principal");

			assertEquals("encoded-principal", TriggerAutomationReactor.principalAccess(user, true, definition(false)));
		}
	}

	private static AutomationDefinitionValidator.ValidatedDefinition definition(boolean durableItems) {
		java.util.Map<String, Object> config = durableItems
				? java.util.Map.of(AutomationConstants.CONFIG_LOOP_DURABLE_ITEMS, true)
				: java.util.Map.of();
		java.util.Map<String, Object> node = java.util.Map.of(
				AutomationConstants.NODE_FIELD_TYPE, AutomationConstants.NODE_CONTROL_LOOP,
				AutomationConstants.NODE_FIELD_CONFIG, config);
		return new AutomationDefinitionValidator.ValidatedDefinition(java.util.Map.of(), java.util.List.of(node),
				java.util.List.of(), "{}", "hash");
	}
}
