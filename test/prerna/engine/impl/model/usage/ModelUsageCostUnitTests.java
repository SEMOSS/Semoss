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
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.jupiter.api.Test;

public class ModelUsageCostUnitTests {

	@Test
	void theEntryIsTheServingProvidersFirst() {
		List<Object> pricing = List.of(entry("openai", "gpt-5", 1.25, 10.0), entry("azure", "gpt-5", 1.5, 12.0));
		Map<String, Object> chosen = ModelUsageCost.priceEntry(pricing, "AZURE_OPEN_AI", "OPENAI", "gpt-5");
		assertEquals("azure", chosen.get(ModelUsageCost.SERVING_PROVIDER));
		assertEquals(1.5, chosen.get(ModelUsageCost.INPUT));
		assertTrue(ModelUsageCost.isServingProviderEntry(chosen, "AZURE_OPEN_AI"));
	}

	@Test
	void aModelServedWhereThereIsNoPriceUsesItsMakersPrice() {
		// a Claude model served through Vertex, with only Anthropic's price
		List<Object> pricing = List.of(entry("azure", "claude-sonnet-4-6", 4.0, 16.0),
				entry("anthropic", "claude-sonnet-4-6", 3.0, 15.0));
		Map<String, Object> chosen = ModelUsageCost.priceEntry(pricing, "VERTEX", "ANTHROPIC", "claude-sonnet-4-6");
		assertEquals("anthropic", chosen.get(ModelUsageCost.SERVING_PROVIDER));
		assertFalse(ModelUsageCost.isServingProviderEntry(chosen, "VERTEX"));
	}

	@Test
	void withNoServingProviderOrMakerPriceTheFirstEntryIsUsed() {
		List<Object> pricing = List.of(entry("openai", "gpt-5", 1.25, 10.0), entry("azure", "gpt-5", 1.5, 12.0));
		assertEquals("openai",
				ModelUsageCost.priceEntry(pricing, "BEDROCK", "gpt-5").get(ModelUsageCost.SERVING_PROVIDER));
		assertEquals("openai",
				ModelUsageCost.priceEntry(pricing, null, null, "gpt-5").get(ModelUsageCost.SERVING_PROVIDER));
		assertNull(ModelUsageCost.priceEntry(List.of(), "BEDROCK", "gpt-5"));
	}

	@Test
	void aServingProviderEntryWithoutRatesFallsBackToTheMakers() {
		Map<String, Object> noRates = new LinkedHashMap<>();
		noRates.put(ModelUsageCost.SERVING_PROVIDER, "google-vertex");
		noRates.put(ModelUsageCost.MODEL_ID, "claude-sonnet-4-6@default");
		List<Object> pricing = List.of(noRates, entry("anthropic", "claude-sonnet-4-6", 3.0, 15.0));
		assertEquals("anthropic", ModelUsageCost.priceEntry(pricing, "VERTEX", "ANTHROPIC", "claude-sonnet-4-6")
				.get(ModelUsageCost.SERVING_PROVIDER));
	}

	@Test
	void aProviderWithSeveralModelsMatchesTheModelId() {
		List<Object> pricing = List.of(entry("google-vertex", "claude-opus-4-8@default", 5.0, 25.0),
				entry("google-vertex", "gemini-3-pro", 2.0, 12.0));
		assertEquals("gemini-3-pro",
				ModelUsageCost.priceEntry(pricing, "VERTEX", "gemini-3-pro").get(ModelUsageCost.MODEL_ID));
		assertEquals("claude-opus-4-8@default",
				ModelUsageCost.priceEntry(pricing, "VERTEX", "claude-opus-4-8").get(ModelUsageCost.MODEL_ID));
		// no match: the first
		assertEquals("claude-opus-4-8@default",
				ModelUsageCost.priceEntry(pricing, "VERTEX", "something-else").get(ModelUsageCost.MODEL_ID));
	}

	@Test
	void knownPricesNeedInputAndOutput() {
		Map<String, Object> inputOnly = new LinkedHashMap<>();
		inputOnly.put(ModelUsageCost.SERVING_PROVIDER, "openai");
		inputOnly.put(ModelUsageCost.MODEL_ID, "gpt-5");
		inputOnly.put(ModelUsageCost.INPUT, 1.25);
		assertNull(ModelUsageCost.priceEntry(List.of(inputOnly), "OPEN_AI", "gpt-5"));
	}

	@Test
	void costPricesEachKindOfTokenAtItsRate() {
		Map<String, Object> entry = entry("openai", "gpt-5", 2.5, 10.0);
		entry.put(ModelUsageCost.CACHE_READ, 1.25);
		entry.put(ModelUsageCost.CACHE_WRITE, 3.75);
		// 1,000 prompt tokens with 200 cache reads and 100 cache writes; 500 response
		// tokens with 100 thinking, priced at the output rate with no reasoning rate
		ModelUsageReport report = new ModelUsageReport(1000, 500, 200, 100, 100, 0);
		long expected = 700 * 25 / 10 + 200 * 125 / 100 + 100 * 375 / 100 + 400 * 10 + 100 * 10;
		assertEquals(expected, ModelUsageCost.costMicros(entry, report));
		assertEquals(7375, ModelUsageCost.costMicros(entry, report));

		entry.put(ModelUsageCost.REASONING, 20.0);
		assertEquals(8375, ModelUsageCost.costMicros(entry, report));
	}

	@Test
	void cacheTokensUseTheInputRateWithoutTheirOwn() {
		Map<String, Object> entry = entry("openai", "gpt-5", 2.0, 8.0);
		ModelUsageReport report = new ModelUsageReport(1000, 0, 400, 100, 0, 0);
		assertEquals(2000, ModelUsageCost.costMicros(entry, report));
	}

	@Test
	void eachCallIsRoundedUpToAWholeMillionth() {
		Map<String, Object> entry = entry("openai", "gpt-5", 0.1, 0.4);
		// exact in decimal: 10 tokens at 0.1 is 1 millionth, not a fraction over it
		assertEquals(1, ModelUsageCost.costMicros(entry, new ModelUsageReport(10, 0, 0, 0, 0, 0)));
		assertEquals(1, ModelUsageCost.costMicros(entry, new ModelUsageReport(3, 0, 0, 0, 0, 0)));
		assertEquals(2, ModelUsageCost.costMicros(entry, new ModelUsageReport(11, 0, 0, 0, 0, 0)));
		assertEquals(0, ModelUsageCost.costMicros(entry, new ModelUsageReport(0, 0, 0, 0, 0, 0)));
	}

	@Test
	void aLongPromptIsPricedAtItsTier() {
		Map<String, Object> entry = entry("anthropic", "claude-opus-4-8", 5.0, 25.0);
		Map<String, Object> tier = new LinkedHashMap<>();
		tier.put(ModelUsageCost.INPUT, 10.0);
		tier.put(ModelUsageCost.OUTPUT, 37.5);
		tier.put(ModelUsageCost.TIER, Map.of("type", "context", "size", 200_000L));
		entry.put(ModelUsageCost.TIERS, List.of(tier));
		assertEquals(100_000L * 5 + 1000L * 25,
				ModelUsageCost.costMicros(entry, new ModelUsageReport(100_000, 1000, 0, 0, 0, 0)));
		assertEquals(250_000L * 10 + 1000L * 375 / 10,
				ModelUsageCost.costMicros(entry, new ModelUsageReport(250_000, 1000, 0, 0, 0, 0)));
	}

	@Test
	void contextOver200kIsATierToo() {
		Map<String, Object> entry = entry("anthropic", "claude-opus-4-8", 5.0, 25.0);
		entry.put(ModelUsageCost.CONTEXT_OVER_200K, Map.of(ModelUsageCost.INPUT, 10.0, ModelUsageCost.OUTPUT, 37.5));
		assertEquals(250_000L * 10, ModelUsageCost.costMicros(entry, new ModelUsageReport(250_000, 0, 0, 0, 0, 0)));
		assertEquals(200_000L * 5, ModelUsageCost.costMicros(entry, new ModelUsageReport(200_000, 0, 0, 0, 0, 0)));
	}

	@Test
	void audioUsesItsOwnRatesWhenThereAreSome() {
		Map<String, Object> entry = entry("openai", "gpt-realtime", 4.0, 16.0);
		entry.put(ModelUsageCost.INPUT_AUDIO, 32.0);
		ModelUsageReport report = new ModelUsageReport(1000, 100, 0, 0, 0, 0).withAudio(400, 50);
		// output audio has no rate of its own, so it is priced at the text output rate
		assertEquals(600 * 4 + 400 * 32 + 100 * 16, ModelUsageCost.costMicros(entry, report));
	}

	@Test
	void embeddingsArePricedPerInputToken() {
		Map<String, Object> entry = entry("openai", "text-embedding-3-small", 0.02, 0.0);
		ModelUsageReport report = new ModelUsageReport(50_000, 0, 0, 0, 0, 0).asEmbeddings();
		assertEquals(1000, ModelUsageCost.costMicros(entry, report));
	}

	@Test
	void theEstimatePricesThePromptAsUncachedAndTheOutputAtTheHigherRate() {
		Map<String, Object> entry = entry("openai", "gpt-5", 2.5, 10.0);
		entry.put(ModelUsageCost.REASONING, 12.0);
		assertEquals(1000L * 25 / 10 + 500L * 12, ModelUsageCost.estimateMicros(entry, 1000, 500L, false));
		// with no output maximum, the default one bounds the estimate
		assertEquals(1000L * 25 / 10 + ModelUsageRequest.DEFAULT_MAX_OUTPUT_TOKENS * 12,
				ModelUsageCost.estimateMicros(entry, 1000, null, false));
		assertEquals(2500L, ModelUsageCost.estimateMicros(entry, 1000, null, true));
	}

	@Test
	void theEstimatePricesThePromptAtTheCacheWriteRateWhenItIsHigher() {
		Map<String, Object> entry = entry("anthropic", "claude-sonnet-4-5", 3.0, 15.0);
		entry.put(ModelUsageCost.CACHE_WRITE, 3.75);
		assertEquals(1000L * 375 / 100 + 100L * 15, ModelUsageCost.estimateMicros(entry, 1000, 100L, false));
		// embeddings are billed at the input rate, and estimated at it
		assertEquals(3000L, ModelUsageCost.estimateMicros(entry, 1000, null, true));
		// a cache write rate below the input rate leaves the input rate
		entry.put(ModelUsageCost.CACHE_WRITE, 1.0);
		assertEquals(1000L * 3 + 100L * 15, ModelUsageCost.estimateMicros(entry, 1000, 100L, false));
	}

	@Test
	void totalsArePricedAtTheBaseRatesWhateverTheirSize() {
		Map<String, Object> entry = entry("anthropic", "claude-opus-4-8", 5.0, 25.0);
		entry.put(ModelUsageCost.CONTEXT_OVER_200K, Map.of(ModelUsageCost.INPUT, 10.0, ModelUsageCost.OUTPUT, 37.5));
		ModelUsageReport day = new ModelUsageReport(250_000, 1000, 0, 0, 0, 0);
		assertEquals(250_000L * 5 + 1000L * 25, ModelUsageCost.costMicrosAtBaseRates(entry, day));
		// one call that long is priced at its tier
		assertEquals(250_000L * 10 + 1000L * 375 / 10, ModelUsageCost.costMicros(entry, day));
		assertEquals(0, ModelUsageCost.costMicrosAtBaseRates(null, day));
	}

	@Test
	void pythonRatesAreDollarsPerMillionTokens() {
		Map<String, Object> entry = entry("openai", "gpt-5", 2.5, 10.0);
		entry.put(ModelUsageCost.CACHE_READ, 1.25);
		Map<String, Object> rates = ModelUsageCost.pythonRates(entry, 1000);
		assertEquals(2.5, rates.get(ModelUsageCost.INPUT));
		assertEquals(10.0, rates.get(ModelUsageCost.OUTPUT));
		assertEquals(1.25, rates.get(ModelUsageCost.CACHE_READ));
		assertNull(rates.get(ModelUsageCost.CACHE_WRITE));
		assertNull(rates.get(ModelUsageCost.REASONING));
	}

	private static Map<String, Object> entry(String provider, String modelId, double input, double output) {
		Map<String, Object> entry = new LinkedHashMap<>();
		entry.put(ModelUsageCost.SERVING_PROVIDER, provider);
		entry.put(ModelUsageCost.MODEL_ID, modelId);
		entry.put(ModelUsageCost.INPUT, input);
		entry.put(ModelUsageCost.OUTPUT, output);
		return entry;
	}
}
