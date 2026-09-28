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
package prerna.reactor.automation;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

/** Covers downstream selection for the destructive branch-removal option. */
public class RemoveAutomationStepUnitTests {

	@Test
	void includesEveryReachableBranchAndMergeNode() {
		List<Map<String, Object>> edges = List.of(edge("start", "query"), edge("query", "decision"),
				edge("decision", "left"), edge("decision", "right"), edge("left", "merge"),
				edge("right", "merge"));

		assertEquals(List.of("decision", "left", "right", "merge"),
				new ArrayList<>(RemoveAutomationStepReactor.downstreamNodeIds(edges, "decision")));
	}

	@Test
	void deletingOneBranchAlsoSelectsItsSharedDownstreamMerge() {
		List<Map<String, Object>> edges = List.of(edge("decision", "left"), edge("decision", "right"),
				edge("left", "merge"), edge("right", "merge"));

		assertEquals(List.of("left", "merge"),
				new ArrayList<>(RemoveAutomationStepReactor.downstreamNodeIds(edges, "left")));
	}

	private static Map<String, Object> edge(String source, String target) {
		return Map.of(AutomationConstants.EDGE_FIELD_KIND, AutomationConstants.EDGE_KIND_CONTROL,
				AutomationConstants.EDGE_FIELD_SOURCE, source, AutomationConstants.EDGE_FIELD_TARGET, target);
	}
}
