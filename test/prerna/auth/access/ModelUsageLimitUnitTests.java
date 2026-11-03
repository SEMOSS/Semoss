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
package prerna.auth.access;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;

import org.junit.jupiter.api.Test;

import prerna.auth.access.ModelUsageLimitRule.Counts;
import prerna.auth.access.ModelUsageLimitRule.Metric;
import prerna.auth.access.ModelUsageLimitRule.Window;

class ModelUsageLimitUnitTests {

	@Test
	void unlimitedHasNoRulesAndOfKeepsItsRules() {
		ModelUsageLimit unlimited = ModelUsageLimit.unlimited();
		ModelUsageLimit daily = ModelUsageLimit
				.of(List.of(new ModelUsageLimitRule(Metric.TOKENS, 500L, Window.DAY, Counts.EACH_PERSON)));

		assertTrue(unlimited.isUnlimited());
		assertTrue(unlimited.getRules().isEmpty());
		assertFalse(daily.isUnlimited());
		assertEquals(500L, daily.getRules().get(0).getAmount());
		assertThrows(UnsupportedOperationException.class, () -> daily.getRules().clear());
		assertTrue(new ModelUsageLimit().getRules().isEmpty());
	}

	@Test
	void windowLength_ofOneIsStoredAsNull() {
		assertEquals(1,
				new ModelUsageLimitRule(Metric.TOKENS, 100L, Window.MINUTE, 1, Counts.EACH_PERSON).getWindowLength());
		assertNull(new ModelUsageLimitRule(Metric.TOKENS, 100L, Window.MINUTE, 1, Counts.EACH_PERSON)
				.getWindowLengthOrNull());
	}
}
