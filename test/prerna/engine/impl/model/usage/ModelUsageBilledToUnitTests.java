/*******************************************************************************
' * Copyright 2015 Defense Health Agency (DHA)
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
package prerna.engine.impl.model.usage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class ModelUsageBilledToUnitTests {

	@Test
	void namesEachSourceAsTheUsageLimitsUiDoes() {
		assertEquals("Direct Limit", new ModelUsageBilledTo(ModelUsageBilledTo.USER, "u-1", "NATIVE").label());
		assertEquals("Ops Team Limit", new ModelUsageBilledTo(ModelUsageBilledTo.TEAM, "Ops", "CUSTOM").label());
		assertEquals("Team Limit", new ModelUsageBilledTo(ModelUsageBilledTo.TEAM, null, null).label());
		assertEquals("Public Use Limit", new ModelUsageBilledTo(ModelUsageBilledTo.PUBLIC, null, null).label());
		assertEquals("User's Total Model Limit",
				new ModelUsageBilledTo(ModelUsageBilledTo.ALL_MODELS, "u-1", "NATIVE").label());
		assertEquals("General Platform Limit", new ModelUsageBilledTo(ModelUsageBilledTo.PLATFORM, null, null).label());
		assertEquals("SOMETHING_NEW", new ModelUsageBilledTo("SOMETHING_NEW", null, null).label());
		assertNull(new ModelUsageBilledTo(null, null, null).label());
	}
}
