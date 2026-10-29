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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

import prerna.collaboration.BrainStyleLearning.Sample;

class BrainStyleLearningUnitTests {

	private static final List<Sample> SAMPLES = List.of(
			new Sample("s1", "internal", "Hey team, quick update: the build is green. Thanks, Rob"),
			new Sample("s2", "internal", "Hi all, shipping Friday as planned. Cheers, Rob"),
			new Sample("s3", "internal", "Morning, can someone review my change today? Thanks, Rob"),
			new Sample("s4", "external", "Dear Ms. Chen, thank you for your note. Kind regards, Robert"),
			new Sample("s5", "chat", "please keep it short and skip the greeting"));

	private static Map<String, Object> variant(String summary, List<Map<String, String>> examples) {
		return Map.of("summary", summary, "examples", examples);
	}

	@Test
	void keepsOnlyExamplesQuotedFromTheNamedSample() {
		Map<String, Object> answer = Map.of("summary", "Short and friendly.", "examples", List.of(
				Map.of("sample", "s1", "text", "quick update: the build is green"),
				Map.of("sample", "s2", "text", "this text is not in any sample at all"),
				Map.of("sample", "s9", "text", "quick update: the build is green")),
				"variants", Map.of());
		Map<String, Object> proposal = BrainStyleLearning.check(answer, SAMPLES);
		assertEquals(List.of("quick update: the build is green"), proposal.get("examples"));
	}

	@Test
	void variantsNeedEnoughSamplesAndMatchingAudience() {
		Map<String, Object> answer = Map.of("summary", "General.", "examples", List.of(), "variants", Map.of(
				"internal", variant("Casual.", List.of(Map.of("sample", "s1", "text", "Hey team, quick update"))),
				"external", variant("Formal.", List.of(Map.of("sample", "s4", "text", "thank you for your note")))));
		@SuppressWarnings("unchecked")
		Map<String, Object> variants = (Map<String, Object>) BrainStyleLearning.check(answer, SAMPLES).get("variants");
		assertTrue(variants.containsKey("internal"));
		// one external sample is too few to describe a separate style
		assertTrue(!variants.containsKey("external"));
	}

	@Test
	void anExampleFromAnotherAudienceIsDropped() {
		Map<String, Object> answer = Map.of("summary", "General.", "examples", List.of(), "variants", Map.of(
				"internal", variant("Casual.", List.of(Map.of("sample", "s4", "text", "thank you for your note")))));
		@SuppressWarnings("unchecked")
		Map<String, Object> internal = (Map<String, Object>) ((Map<String, Object>) BrainStyleLearning
				.check(answer, SAMPLES).get("variants")).get("internal");
		assertEquals(List.of(), internal.get("examples"));
	}

	@Test
	void noSummaryMeansNoProposal() {
		assertNull(BrainStyleLearning.check(Map.of("summary", "  ", "examples", List.of(), "variants", Map.of()),
				SAMPLES));
	}

	@Test
	void audienceIsTheRecipientsDomain() {
		assertEquals("acme.com", BrainStyleLearning.domainOf("Rob@ACME.com"));
		assertNull(BrainStyleLearning.domainOf("no-at-sign"));
	}

	@Test
	void cleanVariantsKeepsKnownAudiencesWithASummary() {
		Map<String, Object> cleaned = BrainProfileUtils.cleanVariants(Map.of("internal",
				Map.of("summary", " Casual ", "examples", List.of("Hi all", " ", 7)), "external",
				Map.of("summary", ""), "other", Map.of("summary", "x")));
		assertEquals(List.of("internal"), List.copyOf(cleaned.keySet()));
		@SuppressWarnings("unchecked")
		Map<String, Object> internal = (Map<String, Object>) cleaned.get("internal");
		assertEquals("Casual", internal.get("summary"));
		assertEquals(List.of("Hi all"), internal.get("examples"));
	}
}
