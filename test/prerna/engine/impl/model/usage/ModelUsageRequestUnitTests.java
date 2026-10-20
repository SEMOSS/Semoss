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
package prerna.engine.impl.model.usage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

public class ModelUsageRequestUnitTests {

	@Test
	void thePromptEstimateIsAboveWhatTokenizersCount() {
		// 300 bytes of English is about 75 tokens; the estimate counts 100
		String text = "a".repeat(300);
		assertEquals(100 + 16, ModelUsageRequest.estimateTokens(text));
		// CJK text: three bytes a character, about a token each
		String cjk = "\u4f60".repeat(100);
		assertEquals(100 + 16, ModelUsageRequest.estimateTokens(cjk));
		assertEquals(0, ModelUsageRequest.estimateTokens((Object) null));
	}

	@Test
	void embeddedImagesCountAsAnImageEach() {
		String message = "look at this data:image/png;base64," + "QUJD".repeat(10_000) + " and this";
		long estimate = ModelUsageRequest.estimateTokens(message);
		long text = ("look at this " + " and this").length();
		assertEquals((text + 2) / 3 + ModelUsageRequest.IMAGE_TOKENS + 16, estimate);
	}

	@Test
	void mediaInTheMessageHistoryCountsAsWhatItHolds() {
		String pdf = "JVBER" + "A".repeat(399_995);
		String image = "iVBOR" + "A".repeat(9_995);
		String messages = "[{\"parts\": [{\"type\": \"MEDIA\", \"media\": {\"mimeType\": \"application/pdf\", "
				+ "\"base64Data\": \"" + pdf + "\"}}, {\"type\": \"MEDIA\", \"media\": {\"base64Data\": \"" + image
				+ "\"}}]}]";
		long estimate = ModelUsageRequest.estimateTokens(messages);
		// 300,000 bytes of PDF is six pages; the image is sniffed from its first bytes
		long media = 6 * ModelUsageRequest.PDF_PAGE_TOKENS + ModelUsageRequest.IMAGE_TOKENS;
		assertTrue(estimate >= media + 16, String.valueOf(estimate));
		assertTrue(estimate < media + 100, String.valueOf(estimate));
	}

	@Test
	void anythingThatIsNotTextIsCountedAsJson() {
		long estimate = ModelUsageRequest.estimateTokens(List.of(Map.of("role", "user", "content", "hi")));
		assertTrue(estimate > 16);
	}

	@Test
	void theCallsMostTokensArePromptAndOutput() {
		assertEquals(700L, ModelUsageRequest.ask(200, 500L).estimatedTokens());
		assertEquals(200L, ModelUsageRequest.embeddings(200).estimatedTokens());
		assertEquals(0L, ModelUsageRequest.embeddings(200).getMaxOutputTokens());
		assertFalse(ModelUsageRequest.ask(1, 1L).isRouter());
		assertTrue(ModelUsageRequest.ask(1, 1L).forRouter().isRouter());
		assertEquals(ModelUsageRequest.BATCH_RESERVATION,
				ModelUsageRequest.ask(1, 1L).reservingFor(ModelUsageRequest.BATCH_RESERVATION).getReservation());
	}

	@Test
	void theOutputIsAlwaysBounded() {
		// no caller maximum and no known model maximum, such as a router's own call
		assertEquals(ModelUsageRequest.DEFAULT_MAX_OUTPUT_TOKENS,
				ModelUsageRequest.ask(200, null).getMaxOutputTokens());
		assertEquals(200L + ModelUsageRequest.DEFAULT_MAX_OUTPUT_TOKENS,
				ModelUsageRequest.ask(200, null).estimatedTokens());
		assertEquals(ModelUsageRequest.DEFAULT_MAX_OUTPUT_TOKENS, ModelUsageRequest.ask(200, 0L).getMaxOutputTokens());
		assertEquals(500L, ModelUsageRequest.boundedMaxOutput(500L));
		assertEquals(ModelUsageRequest.DEFAULT_MAX_OUTPUT_TOKENS, ModelUsageRequest.boundedMaxOutput(null));
	}

	@Test
	void thePromptIsEstimatedOnceAndOnlyWhenAskedFor() {
		int[] estimates = { 0 };
		ModelUsageRequest request = ModelUsageRequest.ask(() -> {
			estimates[0]++;
			return 300L;
		}, 100L);
		assertEquals(100L, request.getMaxOutputTokens());
		assertEquals(0, estimates[0]);
		assertEquals(300L, request.getPromptEstimateTokens());
		assertEquals(400L, request.estimatedTokens());
		assertEquals(1, estimates[0]);
	}
}
