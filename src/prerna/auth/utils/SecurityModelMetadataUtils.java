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
import java.time.LocalDate;
import java.time.YearMonth;
import java.time.format.DateTimeParseException;
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
import com.google.gson.GsonBuilder;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.ToNumberPolicy;

import prerna.engine.api.IEngine;
import prerna.engine.api.IRDBMSEngine;
import prerna.engine.api.ModelCapabilityEnum;
import prerna.engine.api.ModelModalityEnum;
import prerna.util.ConnectionUtils;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.StaticModelMetadataCatalog;
import prerna.util.SystemEngineRegistry;
import prerna.util.Utility;

/**
 * Persistence and validation for the one-row-per-engine MODELMETADATA table.
 * Multi-valued and structured fields are stored as JSON in CLOB columns so the
 * security database schema remains portable across supported relational
 * databases.
 */
public final class SecurityModelMetadataUtils extends AbstractSecurityUtils {

	private static final Logger classLogger = LogManager.getLogger(SecurityModelMetadataUtils.class);
	private static final Gson GSON = new Gson();
	private static final Gson LONG_OR_DOUBLE_GSON = new GsonBuilder()
			.setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE).create();

	private static final Set<String> EDITABLE_METADATA_KEYS = Set.of(Constants.MODEL_PROVIDER,
			Constants.SERVING_PROVIDER, Constants.MODEL_CAPABILITY, Constants.INPUT_MODALITIES,
			Constants.OUTPUT_MODALITIES, Constants.CONTEXT_WINDOW, Constants.MAX_TOKENS, Constants.BUILTIN_TOOLS,
			Constants.REASONING, Constants.REASONING_CONFIG, Constants.CATALOG_MODEL_KEY);
	private static final Set<String> CATALOG_ONLY_KEYS = Set.of(Constants.CATALOG_MODEL_KEY,
			Constants.MODEL_PROVIDER, Constants.SERVING_PROVIDER,
			Constants.MODEL_CAPABILITY, Constants.INPUT_MODALITIES, Constants.OUTPUT_MODALITIES,
			Constants.BUILTIN_TOOLS, Constants.MODEL_FAMILY, Constants.ATTACHMENT,
			Constants.REASONING, Constants.TOOL_CALL, Constants.STRUCTURED_OUTPUT, Constants.TEMPERATURE,
			Constants.KNOWLEDGE_CUTOFF, Constants.RELEASE_DATE, Constants.SUPPORTED_PARAMETERS,
			Constants.REASONING_CONFIG, Constants.BENCHMARKS, Constants.DESCR);
	private static final Set<String> REMOVED_METADATA_KEYS = Set.of("LICENSE", "LINKS", "WEIGHTS", "OPEN_WEIGHTS",
			"LAST_UPDATED", Constants.MAX_INPUT_TOKENS);
	private static final Pattern IDENTIFIER_PATTERN = Pattern.compile("^[A-Z][A-Z0-9_]*$");
	private static final Pattern LOWER_SNAKE_CASE_PATTERN = Pattern.compile("^[a-z][a-z0-9_]*$");
	private static final int MODEL_METADATA_QUERY_BATCH_SIZE = 500;

	private SecurityModelMetadataUtils() {
	}

	/**
	 * Validate metadata values and return a normalized copy. Collections and
	 * structured objects are serialized as JSON.
	 */
	public static Map<String, Object> normalizeModelDetails(Map<String, Object> modelDetails) {
		if (modelDetails == null) {
			throw new IllegalArgumentException("Model details cannot be null");
		}

		Map<String, Object> normalized = new LinkedHashMap<>(modelDetails);
		normalized.keySet().removeAll(REMOVED_METADATA_KEYS);
		normalizeStringProperty(normalized, Constants.DESCR, false);
		normalizeStringProperty(normalized, Constants.CATALOG_MODEL_KEY, false);
		normalizeStringProperty(normalized, Constants.MODEL_PROVIDER, true);
		normalizeStringProperty(normalized, Constants.SERVING_PROVIDER, true);
		normalizeStringProperty(normalized, Constants.MODEL_FAMILY, false);
		normalizeCapabilityProperty(normalized);
		normalizeListProperty(normalized, Constants.INPUT_MODALITIES, true);
		normalizeListProperty(normalized, Constants.OUTPUT_MODALITIES, true);
		normalizeBuiltinToolsProperty(normalized);
		normalizeBooleanProperty(normalized, Constants.ATTACHMENT);
		normalizeBooleanProperty(normalized, Constants.REASONING);
		normalizeBooleanProperty(normalized, Constants.TOOL_CALL);
		normalizeBooleanProperty(normalized, Constants.STRUCTURED_OUTPUT);
		normalizeBooleanProperty(normalized, Constants.TEMPERATURE);
		normalizeDateOrMonthProperty(normalized, Constants.KNOWLEDGE_CUTOFF);
		normalizeDateOrMonthProperty(normalized, Constants.RELEASE_DATE);
		normalizeListProperty(normalized, Constants.SUPPORTED_PARAMETERS, false);
		normalizeJsonObjectProperty(normalized, Constants.REASONING_CONFIG);
		normalizeJsonArrayProperty(normalized, Constants.BENCHMARKS);
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
	 * <p>
	 * The SMSS only carries the handful of properties the model engine needs at
	 * runtime, so this runs as a merge: whatever the SMSS defines wins, anything
	 * already saved for the engine is preserved, and the remaining gaps are filled
	 * from the static catalog. Replacing the row outright would blank the catalog
	 * columns every time the engine is loaded.
	 */
	public static void upsertModelMetadata(String engineId, Properties properties) {
		if (properties == null) {
			return;
		}

		Map<String, Object> details = new LinkedHashMap<>();
		copyIfPresent(properties, details, Constants.MODEL);
		copyIfPresent(properties, details, Constants.CATALOG_MODEL_KEY);
		copyIfPresent(properties, details, Constants.MODEL_PROVIDER);
		copyIfPresent(properties, details, Constants.SERVING_PROVIDER);
		copyIfPresent(properties, details, Constants.MODEL_CAPABILITY);
		copyIfPresent(properties, details, Constants.MODEL_FAMILY);
		copyIfPresent(properties, details, Constants.INPUT_MODALITIES);
		copyIfPresent(properties, details, Constants.OUTPUT_MODALITIES);
		copyIfPresent(properties, details, Constants.CONTEXT_WINDOW);
		copyIfPresent(properties, details, Constants.MAX_TOKENS);
		copyIfPresent(properties, details, Constants.BUILTIN_TOOLS);
		copyIfPresent(properties, details, Constants.ATTACHMENT);
		copyIfPresent(properties, details, Constants.REASONING);
		copyIfPresent(properties, details, Constants.TOOL_CALL);
		copyIfPresent(properties, details, Constants.STRUCTURED_OUTPUT);
		copyIfPresent(properties, details, Constants.TEMPERATURE);
		copyIfPresent(properties, details, Constants.KNOWLEDGE_CUTOFF);
		copyIfPresent(properties, details, Constants.RELEASE_DATE);
		copyIfPresent(properties, details, Constants.SUPPORTED_PARAMETERS);
		copyIfPresent(properties, details, Constants.REASONING_CONFIG);
		copyIfPresent(properties, details, Constants.BENCHMARKS);

		Map<String, Object> merged = toDetails(getModelMetadata(engineId));
		merged.putAll(details);
		StaticModelMetadataCatalog.applyStaticDefaults(merged);
		upsertModelMetadata(engineId, merged);
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
				? "UPDATE MODELMETADATA SET MODELID=?, CATALOGMODELKEY=?, MODELPROVIDER=?, SERVINGPROVIDER=?, CAPABILITY=?, FAMILY=?, INPUTMODALITIES=?, OUTPUTMODALITIES=?, CONTEXTWINDOW=?, MAXOUTPUTTOKENS=?, BUILTINTOOLS=?, ATTACHMENT=?, REASONING=?, TOOLCALL=?, STRUCTUREDOUTPUT=?, TEMPERATURE=?, KNOWLEDGECUTOFF=?, RELEASEDATE=?, SUPPORTEDPARAMETERS=?, REASONINGCONFIG=?, BENCHMARKS=? WHERE ENGINEID=?"
				: "INSERT INTO MODELMETADATA (MODELID, CATALOGMODELKEY, MODELPROVIDER, SERVINGPROVIDER, CAPABILITY, FAMILY, INPUTMODALITIES, OUTPUTMODALITIES, CONTEXTWINDOW, MAXOUTPUTTOKENS, BUILTINTOOLS, ATTACHMENT, REASONING, TOOLCALL, STRUCTUREDOUTPUT, TEMPERATURE, KNOWLEDGECUTOFF, RELEASEDATE, SUPPORTEDPARAMETERS, REASONINGCONFIG, BENCHMARKS, ENGINEID) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)";

		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(sql);
			int index = 1;
			setNullableString(ps, index++, metadata.modelId());
			setNullableString(ps, index++, metadata.catalogModelKey());
			setNullableString(ps, index++, metadata.modelProvider());
			setNullableString(ps, index++, metadata.servingProvider());
			setNullableString(ps, index++, metadata.capability());
			setNullableString(ps, index++, metadata.family());
			setNullableString(ps, index++, metadata.inputModalitiesJson());
			setNullableString(ps, index++, metadata.outputModalitiesJson());
			setNullableLong(ps, index++, metadata.contextWindow());
			setNullableLong(ps, index++, metadata.maxOutputTokens());
			setNullableString(ps, index++, metadata.builtinToolsJson());
			setNullableBoolean(ps, index++, metadata.attachment());
			setNullableBoolean(ps, index++, metadata.reasoning());
			setNullableBoolean(ps, index++, metadata.toolCall());
			setNullableBoolean(ps, index++, metadata.structuredOutput());
			setNullableBoolean(ps, index++, metadata.temperature());
			setNullableString(ps, index++, metadata.knowledgeCutoff());
			setNullableString(ps, index++, metadata.releaseDate());
			setNullableString(ps, index++, metadata.supportedParametersJson());
			setNullableString(ps, index++, metadata.reasoningConfigJson());
			setNullableString(ps, index++, metadata.benchmarksJson());
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

		Map<String, Object> merged = toDetails(getModelMetadata(engineId));
		merged.putAll(updates);
		upsertModelMetadata(engineId, merged);
	}

	/**
	 * Reapply the metadata carried inside an engine export onto the engine it was
	 * uploaded as. The export writes whatever {@link #getModelMetadata(String)}
	 * returned, so the values are mapped back to their {@link Constants} keys and
	 * revalidated before being saved.
	 * <p>
	 * This runs as a merge on top of whatever cataloguing the upload already
	 * saved from the smss file and the static catalog. Every value the export
	 * carried wins, and a value the export did not carry is left alone rather than
	 * blanked, so an export made before a column existed cannot erase what the
	 * catalog just filled in.
	 *
	 * @param engineId         the engine as it now exists in this instance
	 * @param exportedMetadata the parsed contents of the exported metadata file
	 */
	public static void restoreModelMetadata(String engineId, Map<String, Object> exportedMetadata) {
		if (engineId == null || engineId.trim().isEmpty()) {
			throw new IllegalArgumentException("Engine id cannot be empty");
		}
		if (exportedMetadata == null || exportedMetadata.isEmpty()) {
			return;
		}

		Map<String, Object> exported = toDetails(exportedMetadata);
		if (exported.isEmpty()) {
			return;
		}
		Map<String, Object> merged = toDetails(getModelMetadata(engineId));
		merged.putAll(exported);
		upsertModelMetadata(engineId.trim(), merged);
	}

	/**
	 * Backfill MODELMETADATA from the static catalog for the requested model
	 * engines, or for every model engine in the security database when no engine
	 * ids are given. This is the bulk version of what
	 * {@link #upsertModelMetadata(String, Properties)} already does for a single
	 * engine as it is catalogued on startup - it exists so a catalog refresh can be
	 * applied to models that were created before the catalog knew about them,
	 * without bouncing the server.
	 * <p>
	 * The default is a gap fill: only columns that are currently empty are
	 * written. Pass force to let the catalog win over values that are already
	 * stored. Engines the catalog cannot speak to are reported rather than
	 * touched, and an engine whose values would not change is never written, so
	 * running this over the full catalog is cheap and repeatable.
	 *
	 * @param engineIds engines to sync, or null/empty for all model engines
	 * @param force     overwrite stored values instead of only filling gaps
	 * @param dryRun    report what would change without writing
	 * @return one result map per engine holding engineId, modelId, catalogModelKey,
	 *         status, and the list of fields that changed
	 */
	public static List<Map<String, Object>> syncModelMetadataFromCatalog(Collection<String> engineIds, boolean force,
			boolean dryRun) {
		List<String> targets = new ArrayList<>();
		if (engineIds == null || engineIds.isEmpty()) {
			targets.addAll(SecurityEngineUtils.getAllEngineIds(List.of(IEngine.CATALOG_TYPE.MODEL.toString())));
		} else {
			for (String engineId : new LinkedHashSet<>(engineIds)) {
				if (engineId != null && !engineId.trim().isEmpty()) {
					targets.add(engineId.trim());
				}
			}
		}

		Map<String, Map<String, Object>> existingByEngine = getModelMetadata(targets);
		List<Map<String, Object>> results = new ArrayList<>();
		for (String engineId : targets) {
			results.add(syncModelMetadataFromCatalog(engineId, existingByEngine.get(engineId), force, dryRun));
		}
		return results;
	}

	private static Map<String, Object> syncModelMetadataFromCatalog(String engineId, Map<String, Object> existingRow,
			boolean force, boolean dryRun) {
		Map<String, Object> result = new LinkedHashMap<>();
		result.put("engineId", engineId);
		result.put("changedFields", new ArrayList<String>());

		Map<String, Object> merged = toDetails(existingRow);
		// an engine with no row yet, or one saved before MODELID was populated, still
		// has the provider model id in its smss file
		String modelId = nullableString(merged.get(Constants.MODEL));
		if (modelId == null) {
			modelId = getModelIdFromSmss(engineId);
			if (modelId != null) {
				merged.put(Constants.MODEL, modelId);
			}
		}
		result.put("modelId", modelId);

		String catalogModelKey = nullableString(merged.get(Constants.CATALOG_MODEL_KEY));
		result.put("catalogModelKey", catalogModelKey);
		String lookupId = catalogModelKey != null ? catalogModelKey : modelId;
		if (lookupId == null) {
			result.put("status", "NO_MODEL_ID");
			return result;
		}

		Map<String, Object> defaults;
		try {
			defaults = StaticModelMetadataCatalog.getStaticDefaults(lookupId);
		} catch (RuntimeException e) {
			classLogger.warn("Unable to read the static catalog for engine {} model {}", engineId, lookupId, e);
			result.put("status", "ERROR");
			return result;
		}
		defaults.remove(Constants.DESCR);
		if (defaults.isEmpty()) {
			result.put("status", "NO_CATALOG_ENTRY");
			return result;
		}

		List<String> changedFields = new ArrayList<>();
		for (Map.Entry<String, Object> entry : defaults.entrySet()) {
			String key = entry.getKey();
			Object current = merged.get(key);
			boolean changed = force ? !sameNormalizedValue(key, current, entry.getValue())
					: nullableString(current) == null;
			if (changed) {
				merged.put(key, entry.getValue());
				changedFields.add(key);
			}
		}

		result.put("changedFields", changedFields);
		if (changedFields.isEmpty()) {
			result.put("status", "NO_CHANGE");
			return result;
		}
		if (dryRun) {
			result.put("status", "WOULD_UPDATE");
			return result;
		}

		try {
			upsertModelMetadata(engineId, merged);
		} catch (RuntimeException e) {
			classLogger.error("Failed to sync model metadata for engine {} from the static catalog", engineId, e);
			result.put("status", "ERROR");
			return result;
		}
		classLogger.info("Synced model metadata for engine {} model {} fields {}", engineId,
				Utility.cleanLogString(lookupId), changedFields);
		result.put("status", "UPDATED");
		return result;
	}

	/**
	 * The provider model id as defined in the engine's smss file. Returns null when
	 * the engine is not catalogued or its smss cannot be read - the sync reports
	 * that rather than failing, since one unreadable smss should not stop the rest.
	 */
	private static String getModelIdFromSmss(String engineId) {
		Object smssFile = DIHelper.getInstance().getEngineProperty(engineId + "_" + Constants.STORE);
		if (smssFile == null) {
			return null;
		}
		try {
			Properties smssProp = Utility.loadProperties(smssFile.toString());
			if (smssProp == null) {
				return null;
			}
			return nullableString(smssProp.getProperty(Constants.MODEL));
		} catch (Exception e) {
			classLogger.warn("Unable to read the smss file for engine {}", engineId, e);
			return null;
		}
	}

	/**
	 * Compare a stored value against a catalog value. The stored side comes back
	 * from the database as parsed lists and maps while the catalog side is already
	 * normalized, so both are put through the normalizer one key at a time to get
	 * comparable shapes.
	 */
	private static boolean sameNormalizedValue(String key, Object current, Object catalogValue) {
		return String.valueOf(normalizeSingleValue(key, current))
				.equals(String.valueOf(normalizeSingleValue(key, catalogValue)));
	}

	private static Object normalizeSingleValue(String key, Object value) {
		Map<String, Object> single = new LinkedHashMap<>();
		single.put(key, value);
		try {
			return normalizeModelDetails(single).get(key);
		} catch (RuntimeException e) {
			// a value already stored may not survive current validation - treat it as
			// different so force mode replaces it
			return value;
		}
	}

	/**
	 * Convert a stored metadata row back into the {@link Constants} keyed shape the
	 * upsert accepts. Returns an empty map when the engine has no row yet.
	 * <p>
	 * Values that are not set are left out rather than mapped to an explicit null.
	 * The upsert reads the map by key and writes SQL NULL either way, so the two
	 * are equivalent there, but it lets a caller merging two of these maps tell an
	 * unset value apart from one that is genuinely empty.
	 */
	private static Map<String, Object> toDetails(Map<String, Object> existing) {
		Map<String, Object> details = new LinkedHashMap<>();
		if (existing == null) {
			return details;
		}
		putIfNotNull(details, Constants.MODEL, existing.get("modelId"));
		putIfNotNull(details, Constants.CATALOG_MODEL_KEY, existing.get("catalogModelKey"));
		putIfNotNull(details, Constants.MODEL_PROVIDER, existing.get("modelProvider"));
		putIfNotNull(details, Constants.SERVING_PROVIDER, existing.get("servingProvider"));
		putIfNotNull(details, Constants.MODEL_CAPABILITY, existing.get("capability"));
		putIfNotNull(details, Constants.MODEL_FAMILY, existing.get("family"));
		putIfNotNull(details, Constants.INPUT_MODALITIES, existing.get("inputModalities"));
		putIfNotNull(details, Constants.OUTPUT_MODALITIES, existing.get("outputModalities"));
		putIfNotNull(details, Constants.CONTEXT_WINDOW, existing.get("contextWindow"));
		putIfNotNull(details, Constants.MAX_TOKENS, existing.get("maxOutputTokens"));
		putIfNotNull(details, Constants.BUILTIN_TOOLS, existing.get("builtinTools"));
		putIfNotNull(details, Constants.ATTACHMENT, existing.get("attachment"));
		putIfNotNull(details, Constants.REASONING, existing.get("reasoning"));
		putIfNotNull(details, Constants.TOOL_CALL, existing.get("toolCall"));
		putIfNotNull(details, Constants.STRUCTURED_OUTPUT, existing.get("structuredOutput"));
		putIfNotNull(details, Constants.TEMPERATURE, existing.get("temperature"));
		putIfNotNull(details, Constants.KNOWLEDGE_CUTOFF, existing.get("knowledgeCutoff"));
		putIfNotNull(details, Constants.RELEASE_DATE, existing.get("releaseDate"));
		putIfNotNull(details, Constants.SUPPORTED_PARAMETERS, existing.get("supportedParameters"));
		putIfNotNull(details, Constants.REASONING_CONFIG, existing.get("reasoningConfig"));
		putIfNotNull(details, Constants.BENCHMARKS, existing.get("benchmarks"));
		return details;
	}

	private static void putIfNotNull(Map<String, Object> details, String key, Object value) {
		if (value != null) {
			details.put(key, value);
		}
	}

	/**
	 * Return normalized metadata values, or null when the engine has no metadata.
	 */
	public static Map<String, Object> getModelMetadata(String engineId) {
		IRDBMSEngine securityDb = SystemEngineRegistry.getSecurityDb();
		String sql = "SELECT ENGINEID, MODELID, CATALOGMODELKEY, MODELPROVIDER, SERVINGPROVIDER, CAPABILITY, FAMILY, INPUTMODALITIES, OUTPUTMODALITIES, CONTEXTWINDOW, MAXOUTPUTTOKENS, BUILTINTOOLS, ATTACHMENT, REASONING, TOOLCALL, STRUCTUREDOUTPUT, TEMPERATURE, KNOWLEDGECUTOFF, RELEASEDATE, SUPPORTEDPARAMETERS, REASONINGCONFIG, BENCHMARKS FROM MODELMETADATA WHERE ENGINEID=?";
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
			String sql = "SELECT ENGINEID, MODELID, CATALOGMODELKEY, MODELPROVIDER, SERVINGPROVIDER, CAPABILITY, FAMILY, INPUTMODALITIES, OUTPUTMODALITIES, CONTEXTWINDOW, MAXOUTPUTTOKENS, BUILTINTOOLS, ATTACHMENT, REASONING, TOOLCALL, STRUCTUREDOUTPUT, TEMPERATURE, KNOWLEDGECUTOFF, RELEASEDATE, SUPPORTEDPARAMETERS, REASONINGCONFIG, BENCHMARKS FROM MODELMETADATA WHERE ENGINEID IN ("
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

	/**
	 * Return the public capabilities response shape for model catalog endpoints.
	 * Null map values are omitted by the Pixel JSON serializer, so optional scalar
	 * values use an empty string and optional collections use an empty list when
	 * they have not been populated.
	 */
	public static Map<String, Object> toCapabilities(Map<String, Object> modelMetadata) {
		if (modelMetadata == null) {
			return null;
		}

		Map<String, Object> capabilities = new LinkedHashMap<>();
		capabilities.put("modelId", emptyStringIfNull(modelMetadata.get("modelId")));
		capabilities.put("capability", emptyStringIfNull(modelMetadata.get("capability")));
		capabilities.put("family", emptyStringIfNull(modelMetadata.get("family")));
		capabilities.put("inputModalities", emptyListIfNull(modelMetadata.get("inputModalities")));
		capabilities.put("outputModalities", emptyListIfNull(modelMetadata.get("outputModalities")));
		capabilities.put("contextWindow", emptyStringIfNull(modelMetadata.get("contextWindow")));
		capabilities.put("maxOutputTokens", emptyStringIfNull(modelMetadata.get("maxOutputTokens")));
		capabilities.put("builtinTools", emptyMapIfNull(modelMetadata.get("builtinTools")));
		capabilities.put("attachment", modelMetadata.get("attachment"));
		capabilities.put("reasoning", modelMetadata.get("reasoning"));
		capabilities.put("toolCall", modelMetadata.get("toolCall"));
		capabilities.put("structuredOutput", modelMetadata.get("structuredOutput"));
		capabilities.put("temperature", modelMetadata.get("temperature"));
		capabilities.put("knowledgeCutoff", emptyStringIfNull(modelMetadata.get("knowledgeCutoff")));
		capabilities.put("releaseDate", emptyStringIfNull(modelMetadata.get("releaseDate")));
		capabilities.put("supportedParameters", emptyListIfNull(modelMetadata.get("supportedParameters")));
		capabilities.put("reasoningConfig", emptyMapIfNull(modelMetadata.get("reasoningConfig")));
		capabilities.put("benchmarks", emptyListIfNull(modelMetadata.get("benchmarks")));
		return capabilities;
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
		return details.containsKey(Constants.MODEL) || details.containsKey(Constants.CATALOG_MODEL_KEY)
				|| details.containsKey(Constants.MODEL_PROVIDER)
				|| details.containsKey(Constants.SERVING_PROVIDER) || details.containsKey(Constants.MODEL_CAPABILITY)
				|| details.containsKey(Constants.MODEL_FAMILY)
				|| details.containsKey(Constants.INPUT_MODALITIES) || details.containsKey(Constants.OUTPUT_MODALITIES)
				|| details.containsKey(Constants.CONTEXT_WINDOW)
				|| details.containsKey(Constants.MAX_TOKENS) || details.containsKey(Constants.BUILTIN_TOOLS)
				|| details.containsKey(Constants.ATTACHMENT) || details.containsKey(Constants.REASONING)
				|| details.containsKey(Constants.TOOL_CALL) || details.containsKey(Constants.STRUCTURED_OUTPUT)
				|| details.containsKey(Constants.TEMPERATURE) || details.containsKey(Constants.KNOWLEDGE_CUTOFF)
				|| details.containsKey(Constants.RELEASE_DATE) || details.containsKey(Constants.SUPPORTED_PARAMETERS)
				|| details.containsKey(Constants.REASONING_CONFIG) || details.containsKey(Constants.BENCHMARKS);
	}

	/**
	 * Blank SMSS values are treated as "not specified" so an optional property left
	 * empty in the SMSS does not clear a value that is already saved.
	 */
	private static void copyIfPresent(Properties properties, Map<String, Object> details, String key) {
		if (!properties.containsKey(key)) {
			return;
		}
		String value = nullableString(properties.getProperty(key));
		if (value != null) {
			details.put(key, value);
		}
	}

	private static ModelMetadata toMetadata(String engineId, Map<String, Object> details) {
		return new ModelMetadata(engineId, nullableString(details.get(Constants.MODEL)),
				nullableString(details.get(Constants.CATALOG_MODEL_KEY)),
				nullableString(details.get(Constants.MODEL_PROVIDER)), nullableString(details.get(Constants.SERVING_PROVIDER)),
				nullableString(details.get(Constants.MODEL_CAPABILITY)),
				nullableString(details.get(Constants.MODEL_FAMILY)),
				nullableString(details.get(Constants.INPUT_MODALITIES)),
				nullableString(details.get(Constants.OUTPUT_MODALITIES)),
				toNullableLong(details.get(Constants.CONTEXT_WINDOW)),
				toNullableLong(details.get(Constants.MAX_TOKENS)),
				nullableString(details.get(Constants.BUILTIN_TOOLS)),
				toNullableBoolean(details.get(Constants.ATTACHMENT)), toNullableBoolean(details.get(Constants.REASONING)),
				toNullableBoolean(details.get(Constants.TOOL_CALL)),
				toNullableBoolean(details.get(Constants.STRUCTURED_OUTPUT)),
				toNullableBoolean(details.get(Constants.TEMPERATURE)),
				nullableString(details.get(Constants.KNOWLEDGE_CUTOFF)),
				nullableString(details.get(Constants.RELEASE_DATE)),
				nullableString(details.get(Constants.SUPPORTED_PARAMETERS)),
				nullableString(details.get(Constants.REASONING_CONFIG)), nullableString(details.get(Constants.BENCHMARKS)));
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
		case "CHAT", "LLM" -> ModelCapabilityEnum.TEXT_GENERATION.name();
		case "EMBEDDINGS" -> ModelCapabilityEnum.EMBEDDING.name();
		case "TTS", "TEXT_TO_SPEECH" -> ModelCapabilityEnum.SPEECH_SYNTHESIS.name();
		case "STT", "SPEECH_TO_TEXT" -> ModelCapabilityEnum.TRANSCRIPTION.name();
		default -> capability;
		};
		details.put(Constants.MODEL_CAPABILITY, ModelCapabilityEnum.fromName(capability).name());
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
				value = ModelModalityEnum.fromName(value).name();
			} else {
				value = value.toLowerCase(Locale.ROOT).replace('-', '_').replace(' ', '_');
				if (!LOWER_SNAKE_CASE_PATTERN.matcher(value).matches()) {
					throw new IllegalArgumentException("Invalid " + key + " value " + value);
				}
			}
			normalized.add(value);
		}
		// store an unset list as SQL NULL rather than an empty JSON array
		details.put(key, normalized.isEmpty() ? null : GSON.toJson(normalized));
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

	/**
	 * Built-in tools are stored as a JSON object keyed by tool name holding
	 * the selected catalog definition for each tool. An empty selection is
	 * stored as SQL NULL; anything that is not a JSON object is rejected.
	 */
	private static void normalizeBuiltinToolsProperty(Map<String, Object> details) {
		String key = Constants.BUILTIN_TOOLS;
		if (!details.containsKey(key)) {
			return;
		}
		Object value = details.get(key);
		if (value == null || value.toString().trim().isEmpty()) {
			details.put(key, null);
			return;
		}

		JsonElement json;
		try {
			json = value instanceof String ? JsonParser.parseString(value.toString()) : GSON.toJsonTree(value);
		} catch (RuntimeException e) {
			throw new IllegalArgumentException(key + " must be a JSON object keyed by tool name", e);
		}
		if (!json.isJsonObject()) {
			throw new IllegalArgumentException(key + " must be a JSON object keyed by tool name");
		}

		JsonObject selection = json.getAsJsonObject();
		JsonObject normalized = new JsonObject();
		for (Map.Entry<String, JsonElement> entry : selection.entrySet()) {
			String toolName = entry.getKey().trim().toLowerCase(Locale.ROOT).replace('-', '_').replace(' ', '_');
			if (!LOWER_SNAKE_CASE_PATTERN.matcher(toolName).matches()) {
				throw new IllegalArgumentException("Invalid " + key + " tool name " + entry.getKey());
			}
			normalized.add(toolName, entry.getValue());
		}
		// store an unset selection as SQL NULL rather than an empty JSON object
		details.put(key, normalized.size() == 0 ? null : GSON.toJson(normalized));
	}

	/**
	 * Stored built-in tools are a JSON object keyed by tool name; anything
	 * else in the column reads as unset. Whole numbers parse as longs rather
	 * than gson's default doubles, since the selection is forwarded to the
	 * python clients where a max_uses of 5.0 is not the same request as 5.
	 */
	private static Map<?, ?> parseStoredBuiltinTools(String json) {
		if (json == null || !json.trim().startsWith("{")) {
			return null;
		}
		return LONG_OR_DOUBLE_GSON.fromJson(json, Map.class);
	}

	private static void normalizeBooleanProperty(Map<String, Object> details, String key) {
		if (!details.containsKey(key)) {
			return;
		}
		Object value = details.get(key);
		details.put(key, toNullableBoolean(value));
	}

	private static void normalizeDateOrMonthProperty(Map<String, Object> details, String key) {
		if (!details.containsKey(key)) {
			return;
		}
		String value = nullableString(details.get(key));
		if (value == null) {
			details.put(key, null);
			return;
		}
		try {
			details.put(key, LocalDate.parse(value).toString());
		} catch (DateTimeParseException e) {
			try {
				details.put(key, YearMonth.parse(value).toString());
			} catch (DateTimeParseException monthException) {
				throw new IllegalArgumentException(key + " must use the ISO date format YYYY-MM or YYYY-MM-DD",
						monthException);
			}
		}
	}

	private static void normalizeJsonArrayProperty(Map<String, Object> details, String key) {
		if (!details.containsKey(key)) {
			return;
		}
		Object value = details.get(key);
		if (value == null || value.toString().trim().isEmpty()) {
			details.put(key, null);
			return;
		}
		JsonElement json;
		try {
			json = value instanceof String ? JsonParser.parseString(value.toString()) : GSON.toJsonTree(value);
		} catch (RuntimeException e) {
			throw new IllegalArgumentException(key + " must be a valid JSON array", e);
		}
		if (!json.isJsonArray()) {
			throw new IllegalArgumentException(key + " must be a JSON array");
		}
		details.put(key, GSON.toJson(json));
	}

	private static void normalizeJsonObjectProperty(Map<String, Object> details, String key) {
		if (!details.containsKey(key)) {
			return;
		}
		Object value = details.get(key);
		if (value == null || value.toString().trim().isEmpty()) {
			details.put(key, null);
			return;
		}
		JsonElement json;
		try {
			json = value instanceof String ? JsonParser.parseString(value.toString()) : GSON.toJsonTree(value);
		} catch (RuntimeException e) {
			throw new IllegalArgumentException(key + " must be a valid JSON object", e);
		}
		if (!json.isJsonObject()) {
			throw new IllegalArgumentException(key + " must be a JSON object");
		}
		details.put(key, GSON.toJson(json));
	}

	private static List<?> parseStoredJsonArray(String json) {
		return json == null ? null : GSON.fromJson(json, List.class);
	}

	private static Map<?, ?> parseStoredJsonObject(String json) {
		return json == null ? null : GSON.fromJson(json, Map.class);
	}

	private static Map<String, Object> readModelMetadata(ResultSet rs) throws SQLException {
		Map<String, Object> metadata = new LinkedHashMap<>();
		metadata.put("engineId", rs.getString("ENGINEID"));
		metadata.put("modelId", rs.getString("MODELID"));
		metadata.put("catalogModelKey", rs.getString("CATALOGMODELKEY"));
		metadata.put("modelProvider", rs.getString("MODELPROVIDER"));
		metadata.put("servingProvider", rs.getString("SERVINGPROVIDER"));
		metadata.put("capability", rs.getString("CAPABILITY"));
		metadata.put("family", rs.getString("FAMILY"));
		metadata.put("inputModalities", parseStoredList(rs.getString("INPUTMODALITIES")));
		metadata.put("outputModalities", parseStoredList(rs.getString("OUTPUTMODALITIES")));
		metadata.put("contextWindow", getNullableLong(rs, "CONTEXTWINDOW"));
		metadata.put("maxOutputTokens", getNullableLong(rs, "MAXOUTPUTTOKENS"));
		metadata.put("builtinTools", parseStoredBuiltinTools(rs.getString("BUILTINTOOLS")));
		metadata.put("attachment", getNullableBoolean(rs, "ATTACHMENT"));
		metadata.put("reasoning", getNullableBoolean(rs, "REASONING"));
		metadata.put("toolCall", getNullableBoolean(rs, "TOOLCALL"));
		metadata.put("structuredOutput", getNullableBoolean(rs, "STRUCTUREDOUTPUT"));
		metadata.put("temperature", getNullableBoolean(rs, "TEMPERATURE"));
		metadata.put("knowledgeCutoff", rs.getString("KNOWLEDGECUTOFF"));
		metadata.put("releaseDate", rs.getString("RELEASEDATE"));
		metadata.put("supportedParameters", parseStoredList(rs.getString("SUPPORTEDPARAMETERS")));
		metadata.put("reasoningConfig", parseStoredJsonObject(rs.getString("REASONINGCONFIG")));
		metadata.put("benchmarks", parseStoredJsonArray(rs.getString("BENCHMARKS")));
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

	private static Boolean toNullableBoolean(Object value) {
		if (value == null || value.toString().trim().isEmpty()) {
			return null;
		}
		if (value instanceof Boolean booleanValue) {
			return booleanValue;
		}
		String stringValue = value.toString().trim();
		if ("true".equalsIgnoreCase(stringValue) || "1".equals(stringValue)) {
			return Boolean.TRUE;
		}
		if ("false".equalsIgnoreCase(stringValue) || "0".equals(stringValue)) {
			return Boolean.FALSE;
		}
		throw new IllegalArgumentException("Expected a boolean but received " + value);
	}

	private static String nullableString(Object value) {
		if (value == null) {
			return null;
		}
		String stringValue = value.toString().trim();
		return stringValue.isEmpty() ? null : stringValue;
	}

	private static Object emptyStringIfNull(Object value) {
		return value == null ? "" : value;
	}

	private static Object emptyListIfNull(Object value) {
		return value == null ? List.of() : value;
	}

	private static Object emptyMapIfNull(Object value) {
		return value == null ? Map.of() : value;
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

	private static void setNullableBoolean(PreparedStatement ps, int index, Boolean value) throws SQLException {
		if (value == null) {
			ps.setNull(index, Types.BOOLEAN);
		} else {
			ps.setBoolean(index, value);
		}
	}

	private static Long getNullableLong(ResultSet rs, String column) throws SQLException {
		long value = rs.getLong(column);
		return rs.wasNull() ? null : value;
	}

	private static Boolean getNullableBoolean(ResultSet rs, String column) throws SQLException {
		boolean value = rs.getBoolean(column);
		return rs.wasNull() ? null : value;
	}

	private record ModelMetadata(String engineId, String modelId, String catalogModelKey, String modelProvider,
			String servingProvider,
			String capability, String family, String inputModalitiesJson, String outputModalitiesJson, Long contextWindow,
			Long maxOutputTokens, String builtinToolsJson, Boolean attachment, Boolean reasoning,
			Boolean toolCall, Boolean structuredOutput, Boolean temperature, String knowledgeCutoff, String releaseDate,
			String supportedParametersJson, String reasoningConfigJson, String benchmarksJson) {
	}
}
