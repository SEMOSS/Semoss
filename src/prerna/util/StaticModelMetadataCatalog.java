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
package prerna.util;

import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.FileTime;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.text.similarity.LevenshteinDistance;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import prerna.auth.utils.SecurityModelMetadataUtils;

/**
 * Read-only view over the curated model catalog stored in meta/model.json.
 * <p>
 * The catalog is merged from models.dev and OpenRouter, so entries are sparse -
 * any given model may be missing most fields. Everything here treats a missing
 * value as "unknown" rather than an error so a partial catalog entry can still
 * backfill the columns it does know about.
 */
public final class StaticModelMetadataCatalog {

	private static final Logger classLogger = LogManager.getLogger(StaticModelMetadataCatalog.class);
	private static final Gson GSON = new Gson();

	private static final String META_DIRECTORY = "meta";
	private static final String MODEL_METADATA_FILE = "model.json";
	private static final Type MODEL_METADATA_TYPE = new TypeToken<LinkedHashMap<String, Object>>() {
	}.getType();
	private static final Object CACHE_LOCK = new Object();

	private static final Pattern QUALIFIER_PREFIX_PATTERN = Pattern.compile("^[a-z][a-z-]*\\.(?=.)");
	private static final Pattern VERSION_SUFFIX_PATTERN = Pattern.compile("-v\\d+(?::\\d+)?$");
	private static final Pattern DATE_SUFFIX_PATTERN = Pattern.compile("-\\d{4}-?\\d{2}-?\\d{2}$");

	static final String MATCH_KEY = "key";
	static final String MATCH_SCORE = "score";

	private static final double TOKEN_WEIGHT = 0.6;
	private static final double PREFIX_SCORE_FLOOR = 0.75;
	private static final double MAXIMUM_INEXACT_SCORE = 0.99;
	private static final double MINIMUM_MATCH_SCORE = 0.3;

	private static final LevenshteinDistance EDIT_DISTANCE = LevenshteinDistance.getDefaultInstance();
	private static final Comparator<Map<String, Object>> MATCH_ORDER = Comparator
			.comparingDouble((Map<String, Object> match) -> ((Number) match.get(MATCH_SCORE)).doubleValue()).reversed()
			.thenComparing(match -> String.valueOf(match.get(MATCH_KEY)), String.CASE_INSENSITIVE_ORDER);

	private static volatile MetadataCache metadataCache;

	private StaticModelMetadataCatalog() {
	}

	/**
	 * Location of the catalog file within the SEMOSS base folder.
	 */
	public static Path getMetadataFile() {
		String baseFolder = Utility.getBaseFolder();
		if (baseFolder == null || baseFolder.trim().isEmpty()) {
			throw new IllegalStateException("SEMOSS base folder is not configured");
		}
		return Paths.get(baseFolder, META_DIRECTORY, MODEL_METADATA_FILE);
	}

	/**
	 * Return the catalog entry for a model with the nested modalities, limit, and
	 * openrouter objects hoisted to top-level keys. Returns an empty map when the
	 * model is not in the catalog.
	 */
	public static Map<String, Object> getFlattenedMetadata(Path metadataFile, String modelId) {
		JsonObject metadata = findModel(metadataFile, modelId);
		if (metadata == null) {
			return new LinkedHashMap<>();
		}
		return flattenMetadata(metadata);
	}

	/**
	 * Fill in any MODELMETADATA property the caller did not supply using the
	 * catalog entry for {@link Constants#CATALOG_MODEL_KEY} when one was picked by
	 * hand, falling back to the model id held in {@link Constants#MODEL}. Values the
	 * caller did supply are never overwritten, and a catalog value that fails
	 * validation is skipped rather than failing the whole call - the catalog is
	 * hand-maintained data and must not be able to break model creation.
	 *
	 * @param modelDetails mutated in place
	 */
	public static void applyStaticDefaults(Map<String, Object> modelDetails) {
		Path metadataFile;
		try {
			metadataFile = getMetadataFile();
		} catch (RuntimeException e) {
			classLogger.warn("Unable to locate the static model metadata catalog", e);
			return;
		}
		applyStaticDefaults(metadataFile, modelDetails);
	}

	static void applyStaticDefaults(Path metadataFile, Map<String, Object> modelDetails) {
		if (modelDetails == null) {
			return;
		}

		String catalogModelKey = trimToNull(modelDetails.get(Constants.CATALOG_MODEL_KEY));
		String modelId = catalogModelKey != null ? catalogModelKey : trimToNull(modelDetails.get(Constants.MODEL));
		if (modelId == null) {
			return;
		}

		Map<String, Object> defaults;
		try {
			defaults = getStaticDefaults(metadataFile, modelId);
		} catch (RuntimeException e) {
			classLogger.warn("Unable to read static model metadata for model {}", Utility.cleanLogString(modelId), e);
			return;
		}
		if (defaults.isEmpty()) {
			return;
		}

		List<String> applied = new ArrayList<>();
		for (Map.Entry<String, Object> entry : defaults.entrySet()) {
			if (trimToNull(modelDetails.get(entry.getKey())) == null) {
				modelDetails.put(entry.getKey(), entry.getValue());
				applied.add(entry.getKey());
			}
		}
		if (!applied.isEmpty()) {
			classLogger.info("Applied static model metadata for model {} to properties {}",
					Utility.cleanLogString(modelId), applied);
		}
	}

	/**
	 * Translate a catalog entry into normalized MODELMETADATA properties keyed by
	 * the {@link Constants} names. Properties the catalog cannot speak to -
	 * SERVING_PROVIDER and BUILTIN_TOOLS - are never returned.
	 */
	public static Map<String, Object> getStaticDefaults(String modelId) {
		return getStaticDefaults(getMetadataFile(), modelId);
	}

	static Map<String, Object> getStaticDefaults(Path metadataFile, String modelId) {
		if (!Files.isRegularFile(metadataFile)) {
			// the catalog is optional - installs without one simply get no defaults
			return new LinkedHashMap<>();
		}
		JsonObject model = findModel(metadataFile, modelId);
		if (model == null) {
			return new LinkedHashMap<>();
		}

		Map<String, Object> defaults = new LinkedHashMap<>();
		putString(defaults, Constants.DESCR, model, "description");
		putString(defaults, Constants.MODEL_PROVIDER, model, "provider");
		putString(defaults, Constants.MODEL_FAMILY, model, "family");
		putString(defaults, Constants.KNOWLEDGE_CUTOFF, model, "knowledge");
		putString(defaults, Constants.RELEASE_DATE, model, "release_date");
		putBoolean(defaults, Constants.ATTACHMENT, model, "attachment");
		putBoolean(defaults, Constants.REASONING, model, "reasoning");
		putBoolean(defaults, Constants.TOOL_CALL, model, "tool_call");
		putBoolean(defaults, Constants.STRUCTURED_OUTPUT, model, "structured_output");
		putBoolean(defaults, Constants.TEMPERATURE, model, "temperature");

		JsonObject modalities = getObject(model, "modalities");
		List<String> inputModalities = getStringList(modalities, "input");
		List<String> outputModalities = getStringList(modalities, "output");
		if (!inputModalities.isEmpty()) {
			defaults.put(Constants.INPUT_MODALITIES, inputModalities);
		}
		if (!outputModalities.isEmpty()) {
			defaults.put(Constants.OUTPUT_MODALITIES, outputModalities);
		}

		JsonObject limit = getObject(model, "limit");
		putLong(defaults, Constants.CONTEXT_WINDOW, limit, "context", 1);
		putLong(defaults, Constants.MAX_TOKENS, limit, "output", 2);

		String capability = inferCapability(inputModalities, outputModalities, hasPlaceholderOutputLimit(limit));
		if (capability != null) {
			defaults.put(Constants.MODEL_CAPABILITY, capability);
		}

		putJson(defaults, Constants.BENCHMARKS, model, "benchmarks");

		JsonObject openRouter = getObject(model, "openrouter");
		List<String> supportedParameters = getStringList(openRouter, "supported_parameters");
		if (!supportedParameters.isEmpty()) {
			defaults.put(Constants.SUPPORTED_PARAMETERS, supportedParameters);
		}
		putJson(defaults, Constants.REASONING_CONFIG, openRouter, "reasoning");

		return normalizeIndividually(modelId, defaults);
	}

	/**
	 * The catalog is edited by hand, so a single bad value should cost that one
	 * property rather than the entire model. Normalizing one property at a time
	 * isolates the failure.
	 */
	private static Map<String, Object> normalizeIndividually(String modelId, Map<String, Object> defaults) {
		Map<String, Object> normalized = new LinkedHashMap<>();
		for (Map.Entry<String, Object> entry : defaults.entrySet()) {
			Map<String, Object> single = new LinkedHashMap<>();
			single.put(entry.getKey(), entry.getValue());
			try {
				normalized.putAll(SecurityModelMetadataUtils.normalizeModelDetails(single));
			} catch (RuntimeException e) {
				classLogger.warn("Ignoring invalid static model metadata value for {} on model {}", entry.getKey(),
						Utility.cleanLogString(modelId), e);
			}
		}
		return normalized;
	}

	/**
	 * Best-effort capability from the declared modalities, mirroring the inference
	 * the model import screen makes so a model created through the API lands on the
	 * same capability as one created through the wizard.
	 * <p>
	 * Embedding, reranking, and moderation models all declare a text output in the
	 * catalog, so a text output alone is not enough to call something a text
	 * generator. What sets them apart is the placeholder output limit. When nothing
	 * is conclusive the capability is left for the caller to supply.
	 */
	private static String inferCapability(List<String> inputModalities, List<String> outputModalities,
			boolean placeholderOutputLimit) {
		if (outputModalities.isEmpty()) {
			return null;
		}
		if (outputModalities.contains("video")) {
			return "VIDEO_GENERATION";
		}
		if (outputModalities.contains("image")) {
			return "IMAGE_GENERATION";
		}
		if (outputModalities.contains("audio")) {
			return inputModalities.contains("text") ? "SPEECH_SYNTHESIS" : "TRANSCRIPTION";
		}
		if (outputModalities.contains("vector")) {
			return "EMBEDDING";
		}
		if (outputModalities.contains("text")) {
			if (!inputModalities.contains("text") && inputModalities.contains("audio")) {
				return "TRANSCRIPTION";
			}
			if (inputModalities.contains("text") && !placeholderOutputLimit) {
				return "TEXT_GENERATION";
			}
		}
		return null;
	}

	/**
	 * The catalog records an output limit of 0 or 1 for models that do not produce
	 * a text completion at all.
	 */
	private static boolean hasPlaceholderOutputLimit(JsonObject limit) {
		if (limit == null) {
			return false;
		}
		JsonElement output = limit.get("output");
		if (output == null || !output.isJsonPrimitive() || !output.getAsJsonPrimitive().isNumber()) {
			return false;
		}
		return output.getAsLong() < 2;
	}

	static JsonObject findModel(Path metadataFile, String modelId) {
		String catalogKey = findModelKey(metadataFile, modelId);
		if (catalogKey == null) {
			return null;
		}
		// the parsed catalog is cached, so resolving the key and then reading it back
		// costs one map lookup
		return loadMetadata(metadataFile).getAsJsonObject(catalogKey);
	}

	/**
	 * The catalog key a provider model id resolves to, or null when the catalog has
	 * no entry for it. Callers that need to tell the user which entry was matched -
	 * or that nothing was - want this rather than {@link #findModel}.
	 */
	public static String findModelKey(Path metadataFile, String modelId) {
		if (modelId == null || modelId.trim().isEmpty()) {
			return null;
		}

		JsonObject allMetadata = loadMetadata(metadataFile);
		Set<String> lookupIds = getLookupIds(modelId.trim());
		for (String lookupId : lookupIds) {
			JsonElement metadata = allMetadata.get(lookupId);
			if (metadata != null && metadata.isJsonObject()) {
				return lookupId;
			}
			if (metadata != null && !metadata.isJsonNull()) {
				throw new IllegalStateException("Static model metadata entry '" + lookupId + "' must be a JSON object");
			}
		}

		for (Map.Entry<String, JsonElement> entry : allMetadata.entrySet()) {
			JsonElement candidate = entry.getValue();
			if (!candidate.isJsonObject()) {
				continue;
			}
			JsonElement candidateId = candidate.getAsJsonObject().get("id");
			if (candidateId != null && candidateId.isJsonPrimitive() && lookupIds.contains(candidateId.getAsString())) {
				return entry.getKey();
			}
		}
		return null;
	}

	/**
	 * Every model key in the catalog, sorted, so a caller that could not resolve an
	 * id can offer the whole catalog to pick from. Returns an empty list when the
	 * install has no catalog file.
	 */
	public static List<String> getCatalogKeys(Path metadataFile) {
		if (!Files.isRegularFile(metadataFile)) {
			return new ArrayList<>();
		}
		List<String> catalogKeys = new ArrayList<>(loadMetadata(metadataFile).keySet());
		catalogKeys.sort(String.CASE_INSENSITIVE_ORDER);
		return catalogKeys;
	}

	/**
	 * The catalog entries that most resemble the given model id, best first, capped
	 * at {@code limit}. Meant for the case where {@link #findModelKey} came up empty
	 * and the user has to pick an entry themselves, so the returned maps carry
	 * enough to render a suggestion - key, id, name, provider, family - alongside
	 * the score they were ranked on.
	 * <p>
	 * Entries that look nothing like the id are left out entirely, so an id with no
	 * plausible neighbour returns an empty list rather than the arbitrary head of
	 * the catalog.
	 */
	public static List<Map<String, Object>> findClosestMatches(Path metadataFile, String modelId, int limit) {
		if (limit < 1 || modelId == null || modelId.trim().isEmpty() || !Files.isRegularFile(metadataFile)) {
			return new ArrayList<>();
		}

		List<String> normalizedIds = new ArrayList<>();
		for (String lookupId : getLookupIds(modelId.trim())) {
			String normalizedId = normalizeForComparison(lookupId);
			if (!normalizedId.isEmpty() && !normalizedIds.contains(normalizedId)) {
				normalizedIds.add(normalizedId);
			}
		}
		if (normalizedIds.isEmpty()) {
			return new ArrayList<>();
		}

		List<Map<String, Object>> matches = new ArrayList<>();
		for (Map.Entry<String, JsonElement> entry : loadMetadata(metadataFile).entrySet()) {
			JsonElement candidate = entry.getValue();
			if (!candidate.isJsonObject()) {
				continue;
			}
			JsonObject candidateMetadata = candidate.getAsJsonObject();
			double score = scoreCandidate(normalizedIds, entry.getKey(), candidateMetadata);
			if (score < MINIMUM_MATCH_SCORE) {
				continue;
			}

			Map<String, Object> match = new LinkedHashMap<>();
			match.put(MATCH_KEY, entry.getKey());
			putString(match, "id", candidateMetadata, "id");
			putString(match, "name", candidateMetadata, "name");
			putString(match, "provider", candidateMetadata, "provider");
			putString(match, "family", candidateMetadata, "family");
			match.put(MATCH_SCORE, roundScore(score));
			matches.add(match);
		}

		matches.sort(MATCH_ORDER);
		return matches.size() <= limit ? matches : new ArrayList<>(matches.subList(0, limit));
	}

	/**
	 * How closely a catalog entry resembles the id, taking the best of its key and
	 * its provider id - the catalog is keyed on a bare model name while the id
	 * carries a vendor prefix, and the user may have typed either.
	 */
	private static double scoreCandidate(List<String> normalizedIds, String catalogKey, JsonObject candidate) {
		List<String> candidateValues = new ArrayList<>();
		candidateValues.add(catalogKey);
		JsonElement candidateId = candidate.get("id");
		if (candidateId != null && candidateId.isJsonPrimitive()) {
			candidateValues.add(candidateId.getAsString());
		}

		double best = 0;
		for (String candidateValue : candidateValues) {
			String normalizedCandidate = normalizeForComparison(candidateValue);
			if (normalizedCandidate.isEmpty()) {
				continue;
			}
			for (String normalizedId : normalizedIds) {
				best = Math.max(best, similarity(normalizedId, normalizedCandidate));
			}
		}
		return best;
	}

	/**
	 * How alike two already normalized ids are, from 0 to 1. Segment overlap does
	 * most of the work, edit distance separates candidates that share the same
	 * segments, and a shared prefix outranks both.
	 */
	static double similarity(String left, String right) {
		if (left.equals(right)) {
			return 1;
		}

		List<String> leftTokens = tokenize(left);
		List<String> unmatchedRightTokens = tokenize(right);
		int rightTokenCount = unmatchedRightTokens.size();
		int shared = 0;
		for (String leftToken : leftTokens) {
			if (unmatchedRightTokens.remove(leftToken)) {
				shared++;
			}
		}
		int union = leftTokens.size() + rightTokenCount - shared;
		double tokenScore = union == 0 ? 0 : (double) shared / union;

		int longest = Math.max(left.length(), right.length());
		Integer distance = EDIT_DISTANCE.apply(left, right);
		double characterScore = distance == null || longest == 0 ? 0
				: Math.max(0, 1 - ((double) distance / longest));

		double score = TOKEN_WEIGHT * tokenScore + (1 - TOKEN_WEIGHT) * characterScore;
		if (left.startsWith(right) || right.startsWith(left)) {
			int shortest = Math.min(left.length(), right.length());
			score = Math.max(score, PREFIX_SCORE_FLOOR + (1 - PREFIX_SCORE_FLOOR) * ((double) shortest / longest));
		}
		return Math.min(score, MAXIMUM_INEXACT_SCORE);
	}

	/**
	 * Reduce an id to lower case alphanumeric segments joined by single hyphens, so
	 * that "openai/GPT-5.4" and "openai-gpt-5-4" compare as the same shape.
	 * <p>
	 * A letter to digit boundary starts a new segment as well, because whether the
	 * separator gets typed at all is arbitrary - "gpt5" and "gpt-5" are the same
	 * model to everyone but a string comparison.
	 */
	static String normalizeForComparison(String value) {
		if (value == null) {
			return "";
		}
		StringBuilder normalized = new StringBuilder(value.length());
		boolean pendingSeparator = false;
		char previousCharacter = 0;
		for (char character : value.toLowerCase(Locale.ROOT).toCharArray()) {
			if (!Character.isLetterOrDigit(character)) {
				pendingSeparator = true;
				continue;
			}
			if (previousCharacter != 0 && Character.isDigit(character) != Character.isDigit(previousCharacter)) {
				pendingSeparator = true;
			}
			if (pendingSeparator && normalized.length() > 0) {
				normalized.append('-');
			}
			pendingSeparator = false;
			previousCharacter = character;
			normalized.append(character);
		}
		return normalized.toString();
	}

	private static List<String> tokenize(String normalizedId) {
		List<String> tokens = new ArrayList<>();
		for (String token : normalizedId.split("-")) {
			if (!token.isEmpty()) {
				tokens.add(token);
			}
		}
		return tokens;
	}

	/**
	 * The score is a ranking aid, not a measurement - four decimals keeps it
	 * readable in the response without changing any ordering.
	 */
	private static double roundScore(double score) {
		return Math.round(score * 10000d) / 10000d;
	}

	/**
	 * Candidate catalog keys for a provider model id, most specific first.
	 */
	static Set<String> getLookupIds(String modelId) {
		LinkedHashSet<String> lookupIds = new LinkedHashSet<>();
		lookupIds.add(modelId);

		int providerSeparator = modelId.indexOf('/');
		if (providerSeparator >= 0 && providerSeparator < modelId.length() - 1) {
			lookupIds.add(modelId.substring(providerSeparator + 1));
		}

		// peel region/vendor qualifiers one segment at a time
		for (String lookupId : new LinkedHashSet<>(lookupIds)) {
			String remainder = lookupId;
			Matcher matcher = QUALIFIER_PREFIX_PATTERN.matcher(remainder);
			while (matcher.find()) {
				remainder = remainder.substring(matcher.end());
				lookupIds.add(remainder);
				matcher = QUALIFIER_PREFIX_PATTERN.matcher(remainder);
			}
		}

		for (String lookupId : new LinkedHashSet<>(lookupIds)) {
			lookupIds.add(lookupId.replace('@', '-'));
		}
		for (String lookupId : new LinkedHashSet<>(lookupIds)) {
			lookupIds.add(VERSION_SUFFIX_PATTERN.matcher(lookupId).replaceFirst(""));
		}
		for (String lookupId : new LinkedHashSet<>(lookupIds)) {
			lookupIds.add(DATE_SUFFIX_PATTERN.matcher(lookupId).replaceFirst(""));
		}
		return lookupIds;
	}

	private static Map<String, Object> flattenMetadata(JsonObject metadata) {
		JsonObject flattened = metadata.deepCopy();

		JsonElement modalities = metadata.get("modalities");
		if (modalities != null && modalities.isJsonObject()) {
			JsonObject modalityValues = modalities.getAsJsonObject();
			copyJsonProperty(modalityValues, "input", flattened, "input_modalities");
			copyJsonProperty(modalityValues, "output", flattened, "output_modalities");
		}

		JsonElement limit = metadata.get("limit");
		if (limit != null && limit.isJsonObject()) {
			JsonObject limitValues = limit.getAsJsonObject();
			copyJsonProperty(limitValues, "context", flattened, "context_length");
			copyJsonProperty(limitValues, "input", flattened, "max_input_tokens");
			copyJsonProperty(limitValues, "output", flattened, "max_output_tokens");
		}

		copyJsonProperty(metadata, "knowledge", flattened, "knowledge_cutoff");
		JsonElement openRouter = metadata.get("openrouter");
		if (openRouter != null && openRouter.isJsonObject()) {
			JsonObject openRouterValues = openRouter.getAsJsonObject();
			copyJsonProperty(openRouterValues, "supported_parameters", flattened, "supported_parameters");
			copyJsonProperty(openRouterValues, "reasoning", flattened, "reasoning_config");
		}
		return GSON.fromJson(flattened, MODEL_METADATA_TYPE);
	}

	private static void copyJsonProperty(JsonObject source, String sourceKey, JsonObject target, String targetKey) {
		JsonElement value = source.get(sourceKey);
		if (value != null && !value.isJsonNull()) {
			target.add(targetKey, value.deepCopy());
		}
	}

	private static JsonObject getObject(JsonObject source, String key) {
		if (source == null) {
			return null;
		}
		JsonElement value = source.get(key);
		return value != null && value.isJsonObject() ? value.getAsJsonObject() : null;
	}

	private static void putString(Map<String, Object> target, String targetKey, JsonObject source, String sourceKey) {
		if (source == null) {
			return;
		}
		JsonElement value = source.get(sourceKey);
		if (value == null || !value.isJsonPrimitive()) {
			return;
		}
		String stringValue = value.getAsString().trim();
		if (!stringValue.isEmpty()) {
			target.put(targetKey, stringValue);
		}
	}

	private static void putBoolean(Map<String, Object> target, String targetKey, JsonObject source, String sourceKey) {
		if (source == null) {
			return;
		}
		JsonElement value = source.get(sourceKey);
		if (value != null && value.isJsonPrimitive() && value.getAsJsonPrimitive().isBoolean()) {
			target.put(targetKey, value.getAsBoolean());
		}
	}

	private static void putLong(Map<String, Object> target, String targetKey, JsonObject source, String sourceKey,
			long minimum) {
		if (source == null) {
			return;
		}
		JsonElement value = source.get(sourceKey);
		if (value == null || !value.isJsonPrimitive() || !value.getAsJsonPrimitive().isNumber()) {
			return;
		}
		long longValue = value.getAsLong();
		if (longValue >= minimum) {
			target.put(targetKey, longValue);
		}
	}

	private static void putJson(Map<String, Object> target, String targetKey, JsonObject source, String sourceKey) {
		if (source == null) {
			return;
		}
		JsonElement value = source.get(sourceKey);
		if (value == null || value.isJsonNull()) {
			return;
		}
		if (value.isJsonArray() && value.getAsJsonArray().size() == 0) {
			return;
		}
		if (value.isJsonObject() && value.getAsJsonObject().size() == 0) {
			return;
		}
		target.put(targetKey, GSON.toJson(value));
	}

	private static List<String> getStringList(JsonObject source, String key) {
		List<String> values = new ArrayList<>();
		if (source == null) {
			return values;
		}
		JsonElement value = source.get(key);
		if (value == null || !value.isJsonArray()) {
			return values;
		}
		JsonArray array = value.getAsJsonArray();
		for (JsonElement element : array) {
			if (element != null && element.isJsonPrimitive()) {
				String stringValue = element.getAsString().trim().toLowerCase(Locale.ROOT);
				if (!stringValue.isEmpty() && !values.contains(stringValue)) {
					values.add(stringValue);
				}
			}
		}
		return values;
	}

	private static String trimToNull(Object value) {
		if (value == null) {
			return null;
		}
		String stringValue = value.toString().trim();
		return stringValue.isEmpty() ? null : stringValue;
	}

	private static JsonObject loadMetadata(Path metadataFile) {
		Path normalizedPath = metadataFile.toAbsolutePath().normalize();
		try {
			BasicFileAttributes attributes = Files.readAttributes(normalizedPath, BasicFileAttributes.class);
			if (!attributes.isRegularFile()) {
				throw new IllegalStateException("Static model metadata path is not a file: " + normalizedPath);
			}

			MetadataCache currentCache = metadataCache;
			if (currentCache != null && currentCache.matches(normalizedPath, attributes)) {
				return currentCache.metadata();
			}

			synchronized (CACHE_LOCK) {
				attributes = Files.readAttributes(normalizedPath, BasicFileAttributes.class);
				currentCache = metadataCache;
				if (currentCache != null && currentCache.matches(normalizedPath, attributes)) {
					return currentCache.metadata();
				}

				JsonElement parsed;
				try (Reader reader = Files.newBufferedReader(normalizedPath, StandardCharsets.UTF_8)) {
					parsed = JsonParser.parseReader(reader);
				}
				if (!parsed.isJsonObject()) {
					throw new IllegalStateException(
							"Static model metadata file must contain a JSON object: " + normalizedPath);
				}

				JsonObject metadata = parsed.getAsJsonObject();
				metadataCache = new MetadataCache(normalizedPath, attributes.lastModifiedTime(), attributes.size(),
						metadata);
				return metadata;
			}
		} catch (IOException | JsonParseException e) {
			classLogger.error("Unable to read static model metadata from {}", normalizedPath, e);
			throw new IllegalStateException("Unable to read static model metadata from " + normalizedPath, e);
		}
	}

	private record MetadataCache(Path path, FileTime lastModifiedTime, long size, JsonObject metadata) {

		private boolean matches(Path metadataPath, BasicFileAttributes attributes) {
			return this.path.equals(metadataPath) && this.lastModifiedTime.equals(attributes.lastModifiedTime())
					&& this.size == attributes.size();
		}
	}
}
