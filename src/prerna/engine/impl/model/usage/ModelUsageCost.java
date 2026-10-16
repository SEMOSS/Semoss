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

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.benmanes.caffeine.cache.Cache;
import com.github.benmanes.caffeine.cache.Caffeine;

import prerna.auth.utils.SecurityModelMetadataUtils;
import prerna.util.ModelServingProvider;
import prerna.util.StaticBuiltinToolsCatalog;

/**
 * A model's prices and what a call costs at them.
 *
 * <p>
 * The price entry is chosen from {@code MODELMETADATA.PRICING}, providers
 * normalized with {@link StaticBuiltinToolsCatalog#normalizeProviderKey}:
 * </p>
 * <ol>
 * <li>the entry for the engine's serving provider
 * ({@link ModelServingProvider}: its {@code SERVING_PROVIDER}, else the smss
 * {@code PROVIDER}, else its {@code MODEL_TYPE});</li>
 * <li>else the entry for the model's maker ({@code MODELPROVIDER}), such as the
 * Anthropic price of a Claude model served through Vertex;</li>
 * <li>else the first entry.</li>
 * </ol>
 * <p>
 * Within each, the entry whose model id matches the engine's model when there
 * are several, otherwise the first. Only an entry with an {@code input} and an
 * {@code output} rate counts, and a model has known prices when one does.
 * </p>
 *
 * <p>
 * Each model's entry is cached for a minute, so a call does not read
 * {@code MODELMETADATA} each time, and {@link #clearCache(String)} drops it as
 * soon as its prices are saved.
 * </p>
 *
 * <p>
 * Rates are US dollars per million tokens, so tokens times a rate is the cost
 * in millionths of a dollar (micro-dollars, the "micros" of the method names).
 * Costs are counted in whole micro-dollars as {@code long}, and worked out in
 * {@link BigDecimal}, so a large token count times a rate neither overflows nor
 * drifts:
 * </p>
 *
 * <pre>
 * uncached input = prompt tokens - cache read tokens - cache write tokens
 * cost = uncached input * input
 *      + cache read tokens * cache_read     (input when there is no cache_read rate)
 *      + cache write tokens * cache_write   (input when there is no cache_write rate)
 *      + (response tokens - thinking tokens) * output
 *      + thinking tokens * reasoning        (output when there is no reasoning rate)
 * </pre>
 *
 * <p>
 * Each call's cost is rounded up to a whole millionth. A prompt over a tier's
 * size, from {@code tiers} or {@code context_over_200k}, is priced at that
 * tier's rates. Audio uses {@code input_audio} and {@code output_audio} when
 * the entry has them, and the text rates otherwise. Embeddings are priced per
 * input token, and batch calls at the normal price.
 * </p>
 *
 * <p>
 * Before a call, its prompt is priced at the higher of the {@code input} and
 * {@code cache_write} rates, since a client such as Anthropic's marks the
 * prompt for caching itself and a cache write costs more than uncached input.
 * Its output is priced at the higher of the {@code output} and
 * {@code reasoning} rates, so the reservation covers the dearest way the call
 * can bill ({@link #estimateMicros}).
 * </p>
 *
 * <p>
 * Usage added up over many calls, such as the inference log rows written before
 * calls recorded their cost, which seed a counter, is priced at base rates with
 * no tier ({@link #costMicrosAtBaseRates}), since a whole window's prompt
 * tokens would otherwise trip the long-context tier.
 * </p>
 */
public final class ModelUsageCost {

	private static final Logger classLogger = LogManager.getLogger(ModelUsageCost.class);

	// keys of a PRICING entry; the rate keys hold dollars per million tokens
	public static final String SERVING_PROVIDER = "servingProvider";
	public static final String MODEL_ID = "modelId";
	public static final String INPUT = "input";
	public static final String OUTPUT = "output";
	public static final String CACHE_READ = "cache_read";
	public static final String CACHE_WRITE = "cache_write";
	public static final String REASONING = "reasoning";
	public static final String INPUT_AUDIO = "input_audio";
	public static final String OUTPUT_AUDIO = "output_audio";
	public static final String TIERS = "tiers";
	public static final String TIER = "tier";
	public static final String CONTEXT_OVER_200K = "context_over_200k";

	/** The prompt size the {@code context_over_200k} rates apply above */
	private static final long CONTEXT_OVER_200K_SIZE = 200_000L;
	private static final Duration PRICE_CACHE_TIME = Duration.ofMinutes(1);

	/** Each model's price entry, kept briefly; cleared when prices change */
	private static final Cache<String, Optional<Map<String, Object>>> PRICE_ENTRIES = Caffeine.newBuilder()
			.maximumSize(10_000).expireAfterWrite(PRICE_CACHE_TIME).build();

	private ModelUsageCost() {

	}

	/**
	 * Whether a model can be priced. Cost limits apply only to a model that can,
	 * and its calls are billed a cost only then.
	 *
	 * @param engineId the model
	 * @return whether the model has known prices: a price entry
	 *         ({@link #priceEntry}) with an input and an output rate
	 */
	public static boolean hasKnownPrices(String engineId) {
		return priceEntry(engineId) != null;
	}

	/**
	 * The price entry a model's calls are priced at, read from its
	 * {@code MODELMETADATA} row and cached for a minute.
	 *
	 * @param engineId the model
	 * @return the price entry the model is priced at, or null when it has no known
	 *         prices
	 */
	public static Map<String, Object> priceEntry(String engineId) {
		if (engineId == null || engineId.isBlank()) {
			return null;
		}
		// an empty Optional caches "no known prices" too, since the cache keeps no
		// null values and an unpriced model would otherwise be read on every call
		Optional<Map<String, Object>> entry = PRICE_ENTRIES.get(engineId,
				id -> Optional.ofNullable(readPriceEntry(id)));
		return entry == null ? null : entry.orElse(null);
	}

	/**
	 * Forgets a model's cached price entry, for when its prices change.
	 *
	 * @param engineId the model
	 */
	public static void clearCache(String engineId) {
		if (engineId != null) {
			PRICE_ENTRIES.invalidate(engineId);
		}
	}

	/**
	 * Forgets every cached price entry.
	 */
	public static void clearCache() {
		PRICE_ENTRIES.invalidateAll();
	}

	/**
	 * Reads the model's {@code MODELMETADATA} row and picks its price entry. A row
	 * that cannot be read counts as no known prices, so a pricing problem does not
	 * fail the call itself.
	 */
	private static Map<String, Object> readPriceEntry(String engineId) {
		try {
			Map<String, Object> metadata = SecurityModelMetadataUtils.getModelMetadata(engineId);
			if (metadata == null) {
				return null;
			}
			Object pricing = metadata.get("pricing");
			return priceEntry(pricing instanceof List ? (List<?>) pricing : null,
					ModelServingProvider.resolve(engineId, metadata), (String) metadata.get("modelProvider"),
					(String) metadata.get("modelId"));
		} catch (RuntimeException e) {
			classLogger.warn("Unable to read the prices of model " + engineId, e);
			return null;
		}
	}

	/**
	 * {@link #priceEntry(List, String, String, String)} with no model maker
	 */
	public static Map<String, Object> priceEntry(List<?> pricing, String servingProvider, String modelId) {
		return priceEntry(pricing, servingProvider, null, modelId);
	}

	/**
	 * Picks the price entry an engine is priced at from its {@code PRICING}
	 * entries, in the order the class javadoc lists. The entry returned is a
	 * read-only copy.
	 *
	 * @param pricing         the {@code PRICING} entries
	 * @param servingProvider the engine's serving provider, or null
	 * @param modelProvider   the model's maker, or null
	 * @param modelId         the engine's model id, or null
	 * @return the entry the engine is priced at: the serving provider's, else the
	 *         maker's, else the first, each only with an input and an output rate;
	 *         or null when it has no known prices
	 */
	public static Map<String, Object> priceEntry(List<?> pricing, String servingProvider, String modelProvider,
			String modelId) {
		if (pricing == null) {
			return null;
		}
		List<Map<String, Object>> entries = new ArrayList<>();
		for (Object element : pricing) {
			if (element instanceof Map) {
				@SuppressWarnings("unchecked")
				Map<String, Object> entry = (Map<String, Object>) element;
				entries.add(entry);
			}
		}
		// the host bills the call, so its own price comes first; the maker's list
		// price is the closest stand-in when the host has none, such as a Claude
		// model served through Vertex; else only the first entry, which a catalog
		// list keeps as the maker's, so another host's price is never borrowed
		Map<String, Object> chosen = choose(entries, StaticBuiltinToolsCatalog.normalizeProviderKey(servingProvider),
				modelId);
		if (chosen == null) {
			chosen = choose(entries, StaticBuiltinToolsCatalog.normalizeProviderKey(modelProvider), modelId);
		}
		if (chosen == null) {
			chosen = pick(entries, modelId);
		}
		// a read-only copy, since the entry is cached and shared between calls
		return chosen == null ? null : Collections.unmodifiableMap(new LinkedHashMap<>(chosen));
	}

	/**
	 * Whether a price entry is listed under the engine's serving provider,
	 * comparing normalized provider keys.
	 *
	 * @param entry           a price entry
	 * @param servingProvider the engine's serving provider, or null
	 * @return whether the entry is the serving provider's own, rather than the
	 *         maker's or another host's used in its place
	 */
	public static boolean isServingProviderEntry(Map<String, Object> entry, String servingProvider) {
		String provider = StaticBuiltinToolsCatalog.normalizeProviderKey(servingProvider);
		Object entryProvider = entry == null ? null : entry.get(SERVING_PROVIDER);
		return provider != null && entryProvider != null
				&& provider.equals(StaticBuiltinToolsCatalog.normalizeProviderKey(entryProvider.toString()));
	}

	/**
	 * Picks the entry for one provider, from the price entries listed under that
	 * provider.
	 *
	 * @param entries  the price entries
	 * @param provider a normalized provider key, or null
	 * @param modelId  the engine's model id, or null
	 * @return the provider's entry for the model, when it has an input and an
	 *         output rate; or null
	 */
	private static Map<String, Object> choose(List<Map<String, Object>> entries, String provider, String modelId) {
		if (provider == null) {
			return null;
		}
		List<Map<String, Object>> matches = new ArrayList<>();
		for (Map<String, Object> entry : entries) {
			if (isServingProviderEntry(entry, provider)) {
				matches.add(entry);
			}
		}
		return pick(matches, modelId);
	}

	/**
	 * Picks one entry from a list of price entries, preferring the one for the
	 * engine's model when there are several.
	 *
	 * @param entries the price entries to pick from
	 * @param modelId the engine's model id, or null
	 * @return the entry whose model id matches when there are several, otherwise
	 *         the first; or null when there are none or it lacks an input or an
	 *         output rate
	 */
	private static Map<String, Object> pick(List<Map<String, Object>> entries, String modelId) {
		if (entries.isEmpty()) {
			return null;
		}
		Map<String, Object> chosen = entries.get(0);
		if (entries.size() > 1 && modelId != null && !modelId.isBlank()) {
			for (Map<String, Object> entry : entries) {
				if (sameModel(entry.get(MODEL_ID), modelId)) {
					chosen = entry;
					break;
				}
			}
		}
		return rate(chosen, INPUT) == null || rate(chosen, OUTPUT) == null ? null : chosen;
	}

	/**
	 * Whether a price entry's model id names the engine's model: the same id, or
	 * the same {@link #simpleModelId}, since hosts spell one model differently.
	 */
	private static boolean sameModel(Object entryModelId, String modelId) {
		if (entryModelId == null) {
			return false;
		}
		String a = simpleModelId(entryModelId.toString());
		String b = simpleModelId(modelId);
		return entryModelId.toString().equalsIgnoreCase(modelId) || (!a.isEmpty() && a.equals(b));
	}

	/**
	 * The model id without a provider path or a version tag, such as
	 * {@code claude-opus-4-8} for {@code anthropic/claude-opus-4-8@default}.
	 */
	private static String simpleModelId(String modelId) {
		String value = modelId.trim().toLowerCase(Locale.ROOT);
		int slash = value.lastIndexOf('/');
		if (slash >= 0) {
			value = value.substring(slash + 1);
		}
		int at = value.indexOf('@');
		if (at >= 0) {
			value = value.substring(0, at);
		}
		return value;
	}

	/**
	 * Works out the rates a call is priced at, from its price entry and the tier
	 * its prompt size falls into.
	 *
	 * @param entry        a price entry
	 * @param promptTokens the call's prompt tokens
	 * @return the rates the prompt is priced at: the entry's, with those of the
	 *         largest tier whose size the prompt is over laid on top
	 */
	public static Map<String, Object> ratesFor(Map<String, Object> entry, long promptTokens) {
		Map<String, Object> rates = new LinkedHashMap<>(entry);
		Map<String, Object> tierRates = null;
		long tierSize = -1;
		Object tiers = entry.get(TIERS);
		if (tiers instanceof List) {
			for (Object element : (List<?>) tiers) {
				if (!(element instanceof Map)) {
					continue;
				}
				@SuppressWarnings("unchecked")
				Map<String, Object> tier = (Map<String, Object>) element;
				Long size = tierSize(tier);
				if (size != null && promptTokens > size && size > tierSize) {
					tierSize = size;
					tierRates = tier;
				}
			}
		}
		// tiers wins when both are set; context_over_200k is the short form of a
		// single tier at 200k
		if (tierRates == null && promptTokens > CONTEXT_OVER_200K_SIZE && entry.get(CONTEXT_OVER_200K) instanceof Map) {
			@SuppressWarnings("unchecked")
			Map<String, Object> over = (Map<String, Object>) entry.get(CONTEXT_OVER_200K);
			tierRates = over;
		}
		if (tierRates != null) {
			// only the tier's numeric rates replace the base ones, so a rate the tier
			// leaves out stays at its base price
			for (Map.Entry<String, Object> rate : tierRates.entrySet()) {
				if (!TIER.equals(rate.getKey()) && rate.getValue() instanceof Number) {
					rates.put(rate.getKey(), rate.getValue());
				}
			}
		}
		return rates;
	}

	/**
	 * Reads the prompt size a tier applies above from its {@code tier} definition,
	 * for picking the tier a prompt falls into.
	 *
	 * @return the prompt size a tier applies above, or null when it is not a
	 *         context tier; a tier with no type counts as one, since prompt size is
	 *         the only thing a tier is picked by here
	 */
	private static Long tierSize(Map<String, Object> tier) {
		Object definition = tier.get(TIER);
		if (!(definition instanceof Map)) {
			return null;
		}
		Map<?, ?> tierDefinition = (Map<?, ?>) definition;
		Object type = tierDefinition.get("type");
		if (type != null && !"context".equalsIgnoreCase(type.toString())) {
			return null;
		}
		Object size = tierDefinition.get("size");
		return size instanceof Number ? ((Number) size).longValue() : null;
	}

	/**
	 * Works out what one call cost, at the rates of the tier its prompt falls into.
	 *
	 * @param entry  the model's price entry
	 * @param report what the call used
	 * @return what the call cost, in millionths of a dollar, rounded up; 0 with no
	 *         price entry
	 */
	public static long costMicros(Map<String, Object> entry, ModelUsageReport report) {
		if (entry == null || report == null) {
			return 0;
		}
		return costMicrosAt(ratesFor(entry, report.getPromptTokens()), report);
	}

	/**
	 * What usage added up over many calls cost at the entry's base rates. The
	 * prompt tokens of such a total are many calls' together, and a tier picked by
	 * them would price every call as a long prompt, so no tier is used.
	 *
	 * @param entry  the model's price entry
	 * @param report what the calls used together
	 * @return the cost, in millionths of a dollar, rounded up; 0 with no price
	 *         entry
	 */
	public static long costMicrosAtBaseRates(Map<String, Object> entry, ModelUsageReport report) {
		if (entry == null || report == null) {
			return 0;
		}
		return costMicrosAt(entry, report);
	}

	/**
	 * The cost formula of the class javadoc, at the given rates.
	 */
	private static long costMicrosAt(Map<String, Object> rates, ModelUsageReport report) {
		BigDecimal input = rate(rates, INPUT);
		BigDecimal output = rate(rates, OUTPUT);
		if (input == null || output == null) {
			return 0;
		}
		// an embedding call bills its input only
		if (report.isEmbeddings()) {
			return round(input.multiply(BigDecimal.valueOf(report.getPromptTokens())));
		}
		BigDecimal cacheRead = orElse(rate(rates, CACHE_READ), input);
		BigDecimal cacheWrite = orElse(rate(rates, CACHE_WRITE), input);
		BigDecimal reasoning = orElse(rate(rates, REASONING), output);
		BigDecimal inputAudio = rate(rates, INPUT_AUDIO);
		BigDecimal outputAudio = rate(rates, OUTPUT_AUDIO);

		// the prompt is split into cache read, cache write, audio and uncached
		// input, each capped by what is left, so no token is billed twice even when
		// a provider reports parts that add up to more than the prompt; the
		// response is split into thinking, audio and visible output the same way
		long prompt = report.getPromptTokens();
		long cacheReadTokens = Math.min(report.getCacheReadTokens(), prompt);
		long cacheWriteTokens = Math.min(report.getCacheWriteTokens(), prompt - cacheReadTokens);
		long audioIn = Math.min(report.getAudioInputTokens(), prompt - cacheReadTokens - cacheWriteTokens);
		long uncached = Math.max(0, prompt - cacheReadTokens - cacheWriteTokens - audioIn);

		long response = report.getResponseTokens();
		long thinking = Math.min(report.getThinkingTokens(), response);
		long audioOut = Math.min(report.getAudioOutputTokens(), response - thinking);
		long visible = Math.max(0, response - thinking - audioOut);

		if (audioIn > 0 && inputAudio == null) {
			classLogger.warn("Model has no input_audio rate, so audio input is priced at the text input rate");
		}
		if (audioOut > 0 && outputAudio == null) {
			classLogger.warn("Model has no output_audio rate, so audio output is priced at the text output rate");
		}

		BigDecimal cost = input.multiply(BigDecimal.valueOf(uncached))
				.add(cacheRead.multiply(BigDecimal.valueOf(cacheReadTokens)))
				.add(cacheWrite.multiply(BigDecimal.valueOf(cacheWriteTokens)))
				.add(orElse(inputAudio, input).multiply(BigDecimal.valueOf(audioIn)))
				.add(output.multiply(BigDecimal.valueOf(visible))).add(reasoning.multiply(BigDecimal.valueOf(thinking)))
				.add(orElse(outputAudio, output).multiply(BigDecimal.valueOf(audioOut)));
		return round(cost);
	}

	/**
	 * The most a call can cost, for its reservation: the prompt as if nothing were
	 * cached, at the higher of the input and cache write rates, and the output at
	 * the higher of the output and reasoning rates. Embeddings are priced at the
	 * input rate, as they are billed.
	 *
	 * @param entry           the model's price entry
	 * @param promptTokens    the prompt estimate
	 * @param maxOutputTokens the most output, or null for
	 *                        {@link ModelUsageRequest#DEFAULT_MAX_OUTPUT_TOKENS}
	 * @param embeddings      whether the call embeds
	 * @return the estimate in millionths of a dollar, or null when the entry has no
	 *         input or output rate
	 */
	public static Long estimateMicros(Map<String, Object> entry, long promptTokens, Long maxOutputTokens,
			boolean embeddings) {
		if (entry == null) {
			return null;
		}
		Map<String, Object> rates = ratesFor(entry, promptTokens);
		BigDecimal input = rate(rates, INPUT);
		BigDecimal output = rate(rates, OUTPUT);
		if (input == null || output == null) {
			return null;
		}
		if (embeddings) {
			return round(input.multiply(BigDecimal.valueOf(promptTokens)));
		}
		// the dearest rate each side can bill at: the whole prompt may be written to
		// the cache, and all of the output may be thinking
		BigDecimal prompt = input.max(orElse(rate(rates, CACHE_WRITE), input));
		BigDecimal reasoning = orElse(rate(rates, REASONING), output);
		BigDecimal cost = prompt.multiply(BigDecimal.valueOf(promptTokens)).add(output.max(reasoning)
				.multiply(BigDecimal.valueOf(ModelUsageRequest.boundedMaxOutput(maxOutputTokens))));
		return round(cost);
	}

	/**
	 * The rates the Python client fits a call's output to what is left of its cost
	 * with (the {@code rates} of {@link ModelUsageTicket#pythonCeiling()}). Plain
	 * doubles, since they cross into the Python call as plain numbers and only size
	 * the output there; the bill is worked out here from what the call used.
	 *
	 * @param entry        the model's price entry
	 * @param promptTokens the prompt estimate, which picks the tier
	 * @return the rates for the Python call: {@code input}, {@code output},
	 *         {@code cache_read}, {@code cache_write} and {@code reasoning} in
	 *         dollars per million tokens, each null when the entry has none
	 */
	public static Map<String, Object> pythonRates(Map<String, Object> entry, long promptTokens) {
		Map<String, Object> rates = ratesFor(entry, promptTokens);
		Map<String, Object> python = new LinkedHashMap<>();
		for (String key : List.of(INPUT, OUTPUT, CACHE_READ, CACHE_WRITE, REASONING)) {
			BigDecimal rate = rate(rates, key);
			python.put(key, rate == null ? null : rate.doubleValue());
		}
		return python;
	}

	/**
	 * Reads one rate, in dollars per million tokens, from a set of rates as an
	 * exact decimal.
	 *
	 * @return the rate, or null when it is missing, not a number, or negative
	 */
	private static BigDecimal rate(Map<String, Object> rates, String key) {
		Object value = rates.get(key);
		if (!(value instanceof Number)) {
			return null;
		}
		// read through toString, so a double such as 0.3 is the exact 0.3 rather
		// than its binary approximation
		try {
			BigDecimal rate = new BigDecimal(value.toString());
			return rate.signum() < 0 ? null : rate;
		} catch (NumberFormatException e) {
			return null;
		}
	}

	private static BigDecimal orElse(BigDecimal value, BigDecimal fallback) {
		return value == null ? fallback : value;
	}

	/**
	 * Rounds up to a whole micro-dollar, so every billed call costs something and
	 * many tiny calls cannot add up to free usage.
	 */
	private static long round(BigDecimal micros) {
		return micros.setScale(0, RoundingMode.CEILING).longValue();
	}
}
