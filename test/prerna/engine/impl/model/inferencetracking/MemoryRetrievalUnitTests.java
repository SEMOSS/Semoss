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
package prerna.engine.impl.model.inferencetracking;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

class MemoryRetrievalUnitTests {

	@Test
	void replacesSourcesWithLatestCompactedSummaryWithoutChangingRank() {
		List<String> rankedIds = List.of("source-a", "uncompacted", "source-b", "summary-one");
		Map<String, String> replacements = Map.of("source-a", "summary-one", "source-b", "summary-one",
				"summary-one", "summary-two");

		List<String> resolvedIds = MemoryUtils.replaceSupersededMemoryIds(rankedIds, replacements);

		assertEquals(List.of("summary-two", "uncompacted"), resolvedIds);
	}

	@Test
	void terminatesMalformedCompactionCycles() {
		List<String> resolvedIds = MemoryUtils.replaceSupersededMemoryIds(List.of("source-a"),
				Map.of("source-a", "summary-one", "summary-one", "source-a"));

		assertEquals(List.of("source-a"), resolvedIds);
	}
}
