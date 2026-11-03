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
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Set;

import org.junit.jupiter.api.Test;

import prerna.collaboration.BrainRulesGate.Rule;

class BrainRulesGateUnitTests {

	private static final Rule NEVER = new Rule("r-never", "exclude_everywhere", null, null, "p-1", null);
	private static final Rule IGNORE = new Rule("r-ignore", "exclude_topic", null, "t-acme", "p-1", null);

	private static String level(List<Rule> rules, List<String> topicIds, Set<String> vipTopics, boolean vip) {
		return BrainRulesGate.standing(rules, "p1@acme.com", "p-1", null, "email", topicIds, vipTopics, vip).level();
	}

	@Test
	void standingIsNeverThenTopicIgnoreThenTopicVipThenVip() {
		Set<String> vipInAcme = Set.of("t-acme");
		List<String> acme = List.of("t-acme");
		assertEquals(BrainRulesGate.NEVER, level(List.of(IGNORE, NEVER), acme, vipInAcme, true));
		assertEquals("r-never", BrainRulesGate.standing(List.of(IGNORE, NEVER), "p1@acme.com", "p-1", null, "email",
				acme, vipInAcme, true).rule().id());
		assertEquals(BrainRulesGate.EXCLUDED, level(List.of(IGNORE), acme, vipInAcme, true));
		assertEquals(BrainRulesGate.TOPIC_VIP, level(List.of(), acme, vipInAcme, true));
		// the ignore and the topic VIP hold in Acme only; elsewhere the global VIP is what is left
		assertEquals(BrainRulesGate.VIP, level(List.of(IGNORE), List.of("t-beta"), vipInAcme, true));
		assertNull(level(List.of(IGNORE), List.of("t-beta"), vipInAcme, false));
	}
}
