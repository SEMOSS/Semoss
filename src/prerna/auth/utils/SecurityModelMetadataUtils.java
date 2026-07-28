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
package prerna.auth.utils;

import java.math.BigDecimal;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

import prerna.engine.api.IRDBMSEngine;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.SystemEngineRegistry;

/**
 * Persistence and validation for the one-row-per-engine MODELMETADATA table.
 * Multi-valued fields are stored as JSON arrays in CLOB columns so the security
 * database schema remains portable across supported relational databases.
 */
public final class SecurityModelMetadataUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityModelMetadataUtils.class);
	private static final Gson GSON = new Gson();

	private static final Set<String> CAPABILITIES = Set.of("GENERATION", "IMAGE_GENERATION", "VIDEO_GENERATION",
			"EMBEDDING", "TRANSCRIPTION", "SPEECH_SYNTHESIS", "RERANKING", "MODERATION");
	private static final Set<String> MODALITIES = Set.of("TEXT", "IMAGE", "AUDIO", "VIDEO", "VECTOR");
	private static final Set<String> EDITABLE_METADATA_KEYS = Set.of(Constants.MODEL_PROVIDER,
			Constants.SERVING_PROVIDER, Constants.MODEL_CAPABILITY, Constants.INPUT_MODALITIES,
			Constants.OUTPUT_MODALITIES, Constants.CONTEXT_WINDOW, Constants.MAX_TOKENS, Constants.BUILTIN_TOOLS);
	private static final Set<String> CATALOG_ONLY_KEYS = Set.of(Constants.MODEL_PROVIDER, Constants.SERVING_PROVIDER,
			Constants.MODEL_CAPABILITY, Constants.INPUT_MODALITIES, Constants.OUTPUT_MODALITIES,
			Constants.BUILTIN_TOOLS);
	private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("^[A-Z][A-Z0-9_]*$");
	private static final Pattern TOOL_PATTERN = Pattern.compile("^[a-z][a-z0-9_]*$");
	private static final int MODEL_METADATA_QUERY_BATCH_SIZE = 500;

	private SecurityModelMetadataUtils() {
	}

	/**
	 * Validate metadata values and return a normalized copy. Collection values are
	 * serialized as JSON arrays.
	 */
	public static Map<String, Object> normalizeModelDetails(Map<String, Object> modelDetails) {
		if (modelDetails == null) {
			throw new IllegalArgumentException("Model details cannot be null");
		}

		Map<String, Object> normalized = new LinkedHashMap<>(modelDetails);
		normalizeStringProperty(normalized, Constants.MODEL_PROVIDER, true);
		normalizeStringProperty(normalized, Constants.SERVING_PROVIDER, true);
		normalizeCapabilityProperty(normalized);
		normalizeListProperty(normalized, Constants.INPUT_MODALITIES, true);
		normalizeListProperty(normalized, Constants.OUTPUT_MODALITIES, true);
		normalizeListProperty(normalized, Constants.BUILTIN_TOOLS, false);
		normalizePositiveLongProperty(normalized, Constants.CONTEXT_WINDOW);
		normalizePositiveLongProperty(normalized, Constants.MAX_TOKENS);
		return normalized;
	}

	/**
	 * Return the properties needed to open the model engine. Catalog-only metadata
	 * is deliberately excluded so the security database remains its source of
	 * truth. MODEL, CONTEXT_WINDOW, and MAX_TOKENS remain because model engines use
	 * them at runtime.
	 */
	public static Map<String, Object> getModelEngineProperties(Map<String, Object> normalizedModelDetails) {
		Map<String, Object> engineProperties = new LinkedHashMap<>(normalizedModelDetails);
		for (String key : CATALOG_ONLY_KEYS) {
			engineProperties.remove(key);
		}
		return engineProperties;
	}

	/**
	 * Insert or replace the metadata associated with a model engine. If none of the
	 * metadata-related properties are present, no row is created.
	 */
	public static void upsertModelMetadata(String engineId, Properties properties) {
		if (properties == null) {
			return;
		}

		Map<String, Object> details = new LinkedHashMap<>();
		copyIfPresent(properties, details, Constants.MODEL);
		copyIfPresent(properties, details, Constants.MODEL_PROVIDER);
		copyIfPresent(properties, details, Constants.SERVING_PROVIDER);
		copyIfPresent(properties, details, Constants.MODEL_CAPABILITY);
		copyIfPresent(properties, details, Constants.INPUT_MODALITIES);
		copyIfPresent(properties, details, Constants.OUTPUT_MODALITIES);
		copyIfPresent(properties, details, Constants.CONTEXT_WINDOW);
		copyIfPresent(properties, details, Constants.MAX_TOKENS);
		copyIfPresent(properties, details, Constants.BUILTIN_TOOLS);
		upsertModelMetadata(engineId, details);
	}

	/**
	 * Insert or replace the metadata associated with a model engine.
	 */
	public static void upsertModelMetadata(String engineId, Map<String, Object> modelDetails) {
		if (engineId == null || engineId.trim().isEmpty()) {
			throw new IllegalArgumentException("Engine id cannot be empty");
		}
		if (modelDetails == null || !containsMetadata(modelDetails)) {
			return;
		}

		Map<String, Object> normalized = normalizeModelDetails(modelDetails);
		ModelMetadata metadata = toMetadata(engineId.trim(), normalized);
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		boolean exists = modelMetadataExists(securityDb, metadata.engineId());
		String sql = exists
				? "UPDATE MODELMETADATA SET MODELID=?, MODELPROVIDER=?, SERVINGPROVIDER=?, CAPABILITY=?, INPUTMODALITIES=?, OUTPUTMODALITIES=?, CONTEXTWINDOW=?, MAXOUTPUTTOKENS=?, BUILTINTOOLS=? WHERE ENGINEID=?"
				: "INSERT INTO MODELMETADATA (MODELID, MODELPROVIDER, SERVINGPROVIDER, CAPABILITY, INPUTMODALITIES, OUTPUTMODALITIES, CONTEXTWINDOW, MAXOUTPUTTOKENS, BUILTINTOOLS, ENGINEID) VALUES (?,?,?,?,?,?,?,?,?,?)";

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			int index = 1;
			setNullableString(ps, index++, metadata.modelId());
			setNullableString(ps, index++, metadata.modelProvider());
			setNullableString(ps, index++, metadata.servingProvider());
			setNullableString(ps, index++, metadata.capability());
			setNullableString(ps, index++, metadata.inputModalitiesJson());
			setNullableString(ps, index++, metadata.outputModalitiesJson());
			setNullableLong(ps, index++, metadata.contextWindow());
			setNullableLong(ps, index++, metadata.maxOutputTokens());
			setNullableString(ps, index++, metadata.builtinToolsJson());
			ps.setString(index, metadata.engineId());
			ps.executeUpdate();
			ConnectionUtils.commitConnection(ps.getConnection());
		} catch (SQLException e) {
			classLogger.error("Failed to upsert model metadata for engine {}", engineId, e);
			throw new IllegalArgumentException("Failed to save model metadata", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	/**
	 * Update the user-editable metadata while preserving the provider model id,
	 * which is part of the engine's runtime configuration.
	 */
	public static void updateModelMetadata(String engineId, Map<String, Object> updates) {
		if (updates == null) {
			throw new IllegalArgumentException("Model metadata cannot be null");
		}
		for (String key : updates.keySet()) {
			if (!EDITABLE_METADATA_KEYS.contains(key)) {
				throw new IllegalArgumentException("Unallowed model metadata field " + key);
			}
		}

		Map<String, Object> merged = new LinkedHashMap<>();
		Map<String, Object> existing = getModelMetadata(engineId);
		if (existing != null) {
			merged.put(Constants.MODEL, existing.get("modelId"));
			merged.put(Constants.MODEL_PROVIDER, existing.get("modelProvider"));
			merged.put(Constants.SERVING_PROVIDER, existing.get("servingProvider"));
			merged.put(Constants.MODEL_CAPABILITY, existing.get("capability"));
			merged.put(Constants.INPUT_MODALITIES, existing.get("inputModalities"));
			merged.put(Constants.OUTPUT_MODALITIES, existing.get("outputModalities"));
			merged.put(Constants.CONTEXT_WINDOW, existing.get("contextWindow"));
			merged.put(Constants.MAX_TOKENS, existing.get("maxOutputTokens"));
			merged.put(Constants.BUILTIN_TOOLS, existing.get("builtinTools"));
		}
		merged.putAll(updates);
		upsertModelMetadata(engineId, merged);
	}

	/**
	 * Return normalized metadata values, or null when the engine has no metadata.
	 */
	public static Map<String, Object> getModelMetadata(String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String sql = "SELECT ENGINEID, MODELID, MODELPROVIDER, SERVINGPROVIDER, CAPABILITY, INPUTMODALITIES, OUTPUTMODALITIES, CONTEXTWINDOW, MAXOUTPUTTOKENS, BUILTINTOOLS FROM MODELMETADATA WHERE ENGINEID=?";
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			ps.setString(1, engineId);
			rs = ps.executeQuery();
			if (!rs.next()) {
				return null;
			}

			return readModelMetadata(rs);
		} catch (SQLException e) {
			classLogger.error("Failed to retrieve model metadata for engine {}", engineId, e);
			throw new IllegalArgumentException("Failed to retrieve model metadata", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps, rs);
		}
	}

	/**
	 * Return model metadata keyed by engine id for the requested engines. Queries
	 * are batched to avoid database-specific limits on IN-clause parameters.
	 */
	public static Map<String, Map<String, Object>> getModelMetadata(Collection<String> engineIds) {
		Map<String, Map<String, Object>> metadataByEngine = new LinkedHashMap<>();
		if (engineIds == null || engineIds.isEmpty()) {
			return metadataByEngine;
		}

		List<String> normalizedEngineIds = new ArrayList<>();
		for (String engineId : new LinkedHashSet<>(engineIds)) {
			if (engineId != null && !engineId.trim().isEmpty()) {
				normalizedEngineIds.add(engineId.trim());
			}
		}

		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		for (int start = 0; start < normalizedEngineIds.size(); start += MODEL_METADATA_QUERY_BATCH_SIZE) {
			int end = Math.min(start + MODEL_METADATA_QUERY_BATCH_SIZE, normalizedEngineIds.size());
			List<String> batch = normalizedEngineIds.subList(start, end);
			String placeholders = String.join(",", Collections.nCopies(batch.size(), "?"));
			String sql = "SELECT ENGINEID, MODELID, MODELPROVIDER, SERVINGPROVIDER, CAPABILITY, INPUTMODALITIES, OUTPUTMODALITIES, CONTEXTWINDOW, MAXOUTPUTTOKENS, BUILTINTOOLS FROM MODELMETADATA WHERE ENGINEID IN ("
					+ placeholders + ")";

			PreparedStatement ps = null;
			ResultSet rs = null;
			try {
				ps = securityDb.getPreparedStatement(sql);
				for (int i = 0; i < batch.size(); i++) {
					ps.setString(i + 1, batch.get(i));
				}
				rs = ps.executeQuery();
				while (rs.next()) {
					Map<String, Object> metadata = readModelMetadata(rs);
					metadataByEngine.put((String) metadata.get("engineId"), metadata);
				}
			} catch (SQLException e) {
				classLogger.error("Failed to retrieve model metadata for engines", e);
				throw new IllegalArgumentException("Failed to retrieve model metadata", e);
			} finally {
				ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps, rs);
			}
		}
		return metadataByEngine;
	}

	public static void deleteModelMetadata(String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement("DELETE FROM MODELMETADATA WHERE ENGINEID=?");
			ps.setString(1, engineId);
			ps.executeUpdate();
			ConnectionUtils.commitConnection(ps.getConnection());
		} catch (SQLException e) {
			classLogger.error("Failed to delete model metadata for engine {}", engineId, e);
			throw new IllegalArgumentException("Failed to delete model metadata", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps);
		}
	}

	private static boolean containsMetadata(Map<String, Object> details) {
		return details.containsKey(Constants.MODEL) || details.containsKey(Constants.MODEL_PROVIDER)
				|| details.containsKey(Constants.SERVING_PROVIDER) || details.containsKey(Constants.MODEL_CAPABILITY)
				|| details.containsKey(Constants.INPUT_MODALITIES) || details.containsKey(Constants.OUTPUT_MODALITIES)
				|| details.containsKey(Constants.CONTEXT_WINDOW) || details.containsKey(Constants.MAX_TOKENS)
				|| details.containsKey(Constants.BUILTIN_TOOLS);
	}

	private static void copyIfPresent(Properties properties, Map<String, Object> details, String key) {
		if (properties.containsKey(key)) {
			details.put(key, properties.getProperty(key));
		}
	}

	private static ModelMetadata toMetadata(String engineId, Map<String, Object> details) {
		return new ModelMetadata(engineId, nullableString(details.get(Constants.MODEL)),
				nullableString(details.get(Constants.MODEL_PROVIDER)), nullableString(details.get(Constants.SERVING_PROVIDER)),
				nullableString(details.get(Constants.MODEL_CAPABILITY)),
				nullableString(details.get(Constants.INPUT_MODALITIES)),
				nullableString(details.get(Constants.OUTPUT_MODALITIES)),
				toNullableLong(details.get(Constants.CONTEXT_WINDOW)), toNullableLong(details.get(Constants.MAX_TOKENS)),
				nullableString(details.get(Constants.BUILTIN_TOOLS)));
	}

	private static void normalizeStringProperty(Map<String, Object> details, String key, boolean identifier) {
		if (!details.containsKey(key)) {
			return;
		}
		String value = nullableString(details.get(key));
		if (value == null) {
			details.put(key, null);
			return;
		}
		if (identifier) {
			value = value.trim().toUpperCase(Locale.ROOT).replaceAll("[\\s-]+", "_");
			if (!IDENTIFIER_PATTERN.matcher(value).matches()) {
				throw new IllegalArgumentException(key + " must contain only letters, numbers, and underscores");
			}
		}
		details.put(key, value);
	}

	private static void normalizeCapabilityProperty(Map<String, Object> details) {
		if (!details.containsKey(Constants.MODEL_CAPABILITY)) {
			return;
		}
		String capability = nullableString(details.get(Constants.MODEL_CAPABILITY));
		if (capability == null) {
			details.put(Constants.MODEL_CAPABILITY, null);
			return;
		}
		capability = capability.trim().toUpperCase(Locale.ROOT).replace('-', '_').replace(' ', '_');
		capability = switch (capability) {
		case "CHAT", "LLM", "TEXT_GENERATION" -> "GENERATION";
		case "EMBEDDINGS" -> "EMBEDDING";
		case "TTS", "TEXT_TO_SPEECH" -> "SPEECH_SYNTHESIS";
		case "STT", "SPEECH_TO_TEXT" -> "TRANSCRIPTION";
		default -> capability;
		};
		if (!CAPABILITIES.contains(capability)) {
			throw new IllegalArgumentException("Unsupported model capability " + capability);
		}
		details.put(Constants.MODEL_CAPABILITY, capability);
	}

	private static void normalizeListProperty(Map<String, Object> details, String key, boolean modality) {
		if (!details.containsKey(key)) {
			return;
		}
		List<String> values = parseList(details.get(key));
		LinkedHashSet<String> normalized = new LinkedHashSet<>();
		for (String rawValue : values) {
			String value = rawValue.trim();
			if (value.isEmpty()) {
				continue;
			}
			if (modality) {
				value = value.toUpperCase(Locale.ROOT);
				if (!MODALITIES.contains(value)) {
					throw new IllegalArgumentException("Unsupported modality " + value);
				}
			} else {
				value = value.toLowerCase(Locale.ROOT).replace('-', '_').replace(' ', '_');
				if (!TOOL_PATTERN.matcher(value).matches()) {
					throw new IllegalArgumentException("Invalid built-in tool name " + value);
				}
			}
			normalized.add(value);
		}
		details.put(key, GSON.toJson(normalized));
	}

	private static List<String> parseList(Object value) {
		if (value == null) {
			return List.of();
		}
		if (value instanceof Collection<?> collection) {
			List<String> values = new ArrayList<>();
			for (Object item : collection) {
				if (item != null) {
					values.add(item.toString());
				}
			}
			return values;
		}

		String stringValue = value.toString().trim();
		if (stringValue.isEmpty()) {
			return List.of();
		}
		if (stringValue.startsWith("[") && stringValue.endsWith("]")) {
			try {
				JsonArray array = JsonParser.parseString(stringValue).getAsJsonArray();
				List<String> values = new ArrayList<>();
				for (JsonElement element : array) {
					values.add(element.getAsString());
				}
				return values;
			} catch (RuntimeException e) {
				stringValue = stringValue.substring(1, stringValue.length() - 1);
			}
		}
		if (stringValue.isBlank()) {
			return List.of();
		}
		return List.of(stringValue.split("\\s*,\\s*"));
	}

	private static List<String> parseStoredList(String json) {
		return json == null ? null : parseList(json);
	}

	private static Map<String, Object> readModelMetadata(ResultSet rs) throws SQLException {
		Map<String, Object> metadata = new LinkedHashMap<>();
		metadata.put("engineId", rs.getString("ENGINEID"));
		metadata.put("modelId", rs.getString("MODELID"));
		metadata.put("modelProvider", rs.getString("MODELPROVIDER"));
		metadata.put("servingProvider", rs.getString("SERVINGPROVIDER"));
		metadata.put("capability", rs.getString("CAPABILITY"));
		metadata.put("inputModalities", parseStoredList(rs.getString("INPUTMODALITIES")));
		metadata.put("outputModalities", parseStoredList(rs.getString("OUTPUTMODALITIES")));
		metadata.put("contextWindow", getNullableLong(rs, "CONTEXTWINDOW"));
		metadata.put("maxOutputTokens", getNullableLong(rs, "MAXOUTPUTTOKENS"));
		metadata.put("builtinTools", parseStoredList(rs.getString("BUILTINTOOLS")));
		return metadata;
	}

	private static void normalizePositiveLongProperty(Map<String, Object> details, String key) {
		if (!details.containsKey(key)) {
			return;
		}
		Long value = toNullableLong(details.get(key));
		if (value != null && value <= 0) {
			throw new IllegalArgumentException(key + " must be a positive integer");
		}
		// Keep optional empty SMSS values empty rather than serializing the Java null
		// literal, while still persisting them as SQL NULL.
		details.put(key, value == null ? "" : value);
	}

	private static Long toNullableLong(Object value) {
		if (value == null || value.toString().trim().isEmpty()) {
			return null;
		}
		if (value instanceof Number number) {
			try {
				return new BigDecimal(number.toString()).longValueExact();
			} catch (ArithmeticException | NumberFormatException e) {
				throw new IllegalArgumentException("Expected an integer but received " + value, e);
			}
		}
		try {
			return Long.valueOf(value.toString().trim());
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("Expected an integer but received " + value, e);
		}
	}

	private static String nullableString(Object value) {
		if (value == null) {
			return null;
		}
		String stringValue = value.toString().trim();
		return stringValue.isEmpty() ? null : stringValue;
	}

	private static boolean modelMetadataExists(IRDBMSEngine securityDb, String engineId) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			ps = securityDb.getPreparedStatement("SELECT ENGINEID FROM MODELMETADATA WHERE ENGINEID=?");
			ps.setString(1, engineId);
			rs = ps.executeQuery();
			return rs.next();
		} catch (SQLException e) {
			throw new IllegalArgumentException("Failed to inspect model metadata", e);
		} finally {
			ConnectionUtils.closeAllConnectionsIfPooling(securityDb, ps, rs);
		}
	}

	private static void setNullableString(PreparedStatement ps, int index, String value) throws SQLException {
		if (value == null) {
			ps.setNull(index, Types.VARCHAR);
		} else {
			ps.setString(index, value);
		}
	}

	private static void setNullableLong(PreparedStatement ps, int index, Long value) throws SQLException {
		if (value == null) {
			ps.setNull(index, Types.BIGINT);
		} else {
			ps.setLong(index, value);
		}
	}

	private static Long getNullableLong(ResultSet rs, String column) throws SQLException {
		long value = rs.getLong(column);
		return rs.wasNull() ? null : value;
	}

	private record ModelMetadata(String engineId, String modelId, String modelProvider, String servingProvider,
			String capability, String inputModalitiesJson, String outputModalitiesJson, Long contextWindow,
			Long maxOutputTokens, String builtinToolsJson) {
	}
}
