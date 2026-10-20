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
package prerna.engine.impl.model;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.IModelRouterEngine;
import prerna.engine.api.ITypeSafeEngine;
import prerna.engine.api.ModelModalityEnum;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageInputMedia;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.engine.impl.model.responses.TypeSafeModelEngineResponse;
import prerna.engine.impl.model.usage.ModelUsageRequest;
import prerna.om.Insight;
import prerna.util.Constants;
import prerna.util.EngineUtility;
import prerna.util.Utility;

/**
 * <p>
 * Routes each model request to one of several backing {@link IModelEngine model
 * engines}.
 * </p>
 *
 * <p>
 * Routing configuration lives in a JSON file in the engine's assets folder. By
 * convention, the engine loads {@code router.json}. The optional
 * {@value #ROUTER_CONFIG} SMSS property overrides the file name. The SMSS file
 * otherwise needs only the standard engine identity keys:
 * </p>
 *
 * <pre>
 * ENGINE_TYPE = prerna.engine.impl.model.ModelRouterEngine
 * MODEL_TYPE  = MODEL_ROUTER
 * </pre>
 *
 * <h3>router.json schema</h3>
 *
 * <pre>
 * {
 *   "mode": "keyword",                 // "keyword" | "llm" | "typesafe" | "weighted"
 *   "sticky": false,                   // pin a room to its first route (default false)
 *   "default_route": "&lt;engineId&gt;",     // used when no route matches (defaults to route 0)
 *   "fallbacks": ["&lt;engineId&gt;"],       // tried in order when the chosen target fails
 *   "classifier_engine": "&lt;engineId&gt;", // required for "llm" and "typesafe" modes
 *   "embeddings_engine": "&lt;engineId&gt;", // required for the router to serve embeddings
 *   "routes": [
 *     { "name": "code", "engine_id": "aa876e7e-...", "keywords": ["java", "python", "debug"],
 *       "description": "Programming questions: debugging, writing and reviewing code", "weight": 70 },
 *     { "name": "sports", "engine_id": "8380e91f-...", "keywords": ["nba", "nfl", "score"],
 *       "description": "Sports questions: scores, players, teams and schedules", "weight": 30 }
 *   ]
 * }
 * </pre>
 *
 * <h3>Routing modes</h3>
 *
 * <ul>
 * <li><b>keyword</b>: The first route with a whole-word keyword match on the
 * latest input wins. Otherwise, the default route is used.</li>
 * <li><b>llm</b>: The classifier engine selects a route from each route's
 * description. Only chat models are supported; decision models require typesafe
 * mode. Every route requires a description. Keyword matching is used as a
 * fallback when classification fails.</li>
 * <li><b>typesafe</b>: Decision-only classification through ITypeSafeEngine.
 * Route descriptions become choice criteria; failed evaluations use keyword fallback.</li>
 * <li><b>weighted</b>: Deterministic weighted round-robin is applied across
 * routes whose weight is greater than zero. For example, weights of 30 and 70
 * produce an exact 30/70 split during each cycle.</li>
 * </ul>
 *
 * <h3>Sticky routing</h3>
 *
 * <p>
 * Sticky routing is disabled by default. When enabled, the first turn selects a
 * route, and later turns in that room reuse it. This keeps a conversation on one model and
 * avoids repeated classifier calls in {@code llm} mode. In {@code weighted}
 * mode, traffic is therefore divided by conversation rather than by request.
 * </p>
 *
 * <p>
 * Tool results are never routed on: whatever the sticky setting, a tool-result
 * continuation goes to the engine that served the room last, so one agent turn
 * stays on one model.
 * </p>
 *
 * <p>
 * A pin is removed when its engine fails and a fallback serves the request.
 * Pins are stored in a bounded in-memory map per router instance. They reset
 * when the engine reloads and are not shared across nodes.
 * </p>
 *
 * <h3>Failover</h3>
 *
 * <p>
 * When the selected engine cannot load or its request fails, the router tries
 * the configured fallbacks in order and then the default route. The last error
 * is rethrown when every candidate fails.
 * </p>
 *
 * <h3>Access control</h3>
 *
 * <p>
 * Access to the router does not grant access to its backing engines. Every ask
 * and embeddings candidate is checked with
 * {@link SecurityEngineUtils#userCanViewEngine(User, String)}. Candidates the
 * caller cannot access are skipped. The classifier engine is exempt because it
 * is internal routing infrastructure and its output is not returned directly.
 * </p>
 *
 * <h3>Inference logging</h3>
 *
 * <p>
 * Requests are logged once under the router for user-facing attribution and
 * once under the delegated engine for actual model usage. Router rows use
 * {@code route_ask} or {@code route_embeddings}; usage and history queries
 * count only the delegated engine's {@code ask} or {@code embeddings} rows.
 * </p>
 *
 * <h3>Usage limits</h3>
 *
 * <p>
 * Limits on the router and on the model it routes to both apply. The router's
 * own call counts on the router's counters and passes its {@code usage_ceiling}
 * on to the model it routes to, which fits the call in the lesser of the two.
 * The router's own call leaves out the person's all-models and platform
 * counters and its team's total, so a routed call counts once there, under the
 * model that answered. A router has no output maximum of its own, so its own
 * call is reserved for the caller's maximum or
 * {@link ModelUsageRequest#DEFAULT_MAX_OUTPUT_TOKENS}.
 * </p>
 *
 * <h3>Response metadata</h3>
 *
 * <p>
 * The selected target is returned under {@code router_engine_id},
 * {@code routed_engine_id}, and {@code routed_route_name}.
 * </p>
 *
 * <h3>Runtime updates</h3>
 *
 * <p>
 * The {@code GetModelRouterConfig} and {@code UpdateModelRouterConfig} reactors
 * read and update the configuration file. Updates call {@link #reloadConfig()}
 * on the live router instance.
 * </p>
 */
public class ModelRouterEngine extends AbstractModelEngine implements IModelRouterEngine {

	private static final Logger classLogger = LogManager.getLogger(ModelRouterEngine.class);

	/**
	 * Optional SMSS property overriding the config file name in the assets folder.
	 */
	public static final String ROUTER_CONFIG = "ROUTER_CONFIG";
	/** Conventional config file name looked up when ROUTER_CONFIG is not set. */
	public static final String DEFAULT_CONFIG_FILE = "router.json";
	/**
	 * Optional SMSS property holding the initial config JSON inline. Engine
	 * creation from the UI opens the engine before any asset can be uploaded, so
	 * this is read once to seed the config file when it does not exist yet. The
	 * file is the source of truth afterwards - later edits belong in the file.
	 */
	public static final String ROUTER_CONFIG_JSON = "ROUTER_CONFIG_JSON";

	/** Response metadata keys describing the routing decision. */
	public static final String METADATA_ROUTER_ENGINE_ID = "router_engine_id";
	public static final String METADATA_ROUTED_ENGINE_ID = "routed_engine_id";
	public static final String METADATA_ROUTED_ENGINE_NAME = "routed_engine_name";
	public static final String METADATA_ROUTED_ROUTE_NAME = "routed_route_name";
	public static final String METADATA_ROUTING_TRACE = "routing_trace";

	private static final String MODE_KEYWORD = "keyword";
	private static final String MODE_LLM = "llm";
	private static final String MODE_TYPESAFE = "typesafe";
	private static final String MODE_WEIGHTED = "weighted";

	private static final String MESSAGE_JSON = "message_json";

	private static final int MAX_STICKY_ROOMS = 10_000;

	// -------------------------------------------------------------------------
	// Internal route descriptor
	// -------------------------------------------------------------------------
	private static class Route {
		final String name;
		final String engineId;
		final List<String> keywords;
		final int weight;
		/** What the LLM classifier reads; required when mode is llm. */
		final String description;
		/** Whole-word matcher over all keywords; null when the route has none. */
		final Pattern keywordPattern;

		Route(String name, String engineId, List<String> keywords, int weight, String description) {
			this.name = name;
			this.engineId = engineId;
			this.keywords = keywords;
			this.weight = weight;
			this.description = description;
			this.keywordPattern = buildKeywordPattern(keywords);
		}
	}

	private volatile List<Route> routes = Collections.emptyList();
	private volatile String routingMode = MODE_KEYWORD;
	private volatile boolean sticky = false;
	private volatile String defaultRouteEngineId;
	private volatile List<String> fallbackEngineIds = Collections.emptyList();
	private volatile String classifierEngineId;
	private volatile String embeddingsEngineId;
	private volatile int totalWeight = 0;
	/**
	 * Round-robin counter for weighted mode - increments on every weighted call.
	 */
	private final AtomicInteger rrCounter = new AtomicInteger(0);
	/**
	 * LRU of roomId -> engineId that last served the room, read for every input
	 * when sticky is on and for tool-result continuations always.
	 */
	private final Map<String, String> roomRoutePins = Collections
			.synchronizedMap(new LinkedHashMap<String, String>(128, 0.75f, true) {
				@Override
				protected boolean removeEldestEntry(Map.Entry<String, String> eldest) {
					return size() > MAX_STICKY_ROOMS;
				}
			});

	// -------------------------------------------------------------------------
	// IModelEngine
	// -------------------------------------------------------------------------

	@Override
	public ModelTypeEnum getModelType() {
		return ModelTypeEnum.MODEL_ROUTER;
	}

	@Override
	public void close() throws IOException {
	}

	/**
	 * Callers sizing work off this engine (e.g. agent auto-compaction) cannot know
	 * which route will serve them, so answer with the smallest context window among
	 * the serving targets: routes, default route, and fallbacks. An explicit
	 * context window resolved by the base engine still wins. Targets that fail to
	 * load or do not report a window are skipped; when none report one, 0 is
	 * returned. Recompute from the targets so saved limit changes take effect
	 * without also reloading the router's configuration.
	 */
	@Override
	public int getContextWindow() {
		int inherited = super.getContextWindow();
		if (inherited > 0) {
			return inherited;
		}
		return computeMinTargetContextWindow();
	}

	private int computeMinTargetContextWindow() {
		int min = 0;
		for (String targetEngineId : servingEngineIds()) {
			try {
				IModelEngine engine = resolveEngine(targetEngineId);
				int contextWindow = engine.getContextWindow();
				if (contextWindow > 0 && (min == 0 || contextWindow < min)) {
					min = contextWindow;
				}
			} catch (Exception e) {
				classLogger.warn("ModelRouterEngine '{}': could not resolve context window for engineId={}",
						this.engineId, targetEngineId, e);
			}
		}
		return min;
	}

	/** Every engine that could serve an ask: routes, default route, fallbacks. */
	private List<String> servingEngineIds() {
		List<String> ids = new ArrayList<>();
		for (Route route : this.routes) {
			addCandidate(ids, route.engineId);
		}
		addCandidate(ids, this.defaultRouteEngineId);
		for (String fallback : this.fallbackEngineIds) {
			addCandidate(ids, fallback);
		}
		return ids;
	}

	// -------------------------------------------------------------------------
	// Lifecycle and configuration
	// -------------------------------------------------------------------------

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);
		reloadConfig();
		classLogger.info("ModelRouterEngine '{}' loaded: {} route(s), mode={}, sticky={}", this.engineId,
				this.routes.size(), this.routingMode, this.sticky);
	}

	/**
	 * The config file this router reads: assets/&lt;ROUTER_CONFIG or
	 * router.json&gt;.
	 */
	public File resolveConfigFile() {
		String configFileName = this.smssProp.getProperty(ROUTER_CONFIG);
		if (configFileName == null || configFileName.trim().isEmpty()) {
			configFileName = DEFAULT_CONFIG_FILE;
		}
		String assetsFolder = EngineUtility.getSpecificEngineAssetsFolder(IEngine.CATALOG_TYPE.MODEL, this.engineId,
				this.engineName);
		return new File((assetsFolder + "/" + configFileName.trim()).replace("\\", "/"));
	}

	/** Raw contents of the config file, for the settings UI. */
	@Override
	public String readConfigJson() throws IOException {
		File configFile = resolveConfigFile();
		if (!configFile.exists()) {
			throw new IOException("ModelRouterEngine: routing config not found at " + configFile.getAbsolutePath());
		}
		return new String(Files.readAllBytes(configFile.toPath()), StandardCharsets.UTF_8);
	}

	/**
	 * Re-reads and applies the config file on the live instance. Used at open and
	 * after {@link #updateConfig(String)} rewrites the file.
	 */
	@Override
	public synchronized void reloadConfig() throws IOException {
		File configFile = resolveConfigFile();
		if (!configFile.exists()) {
			bootstrapConfigFileIfNeeded(configFile);
		}
		if (!configFile.exists()) {
			throw new IOException("ModelRouterEngine: routing config not found at " + configFile.getAbsolutePath()
					+ ". Place a " + DEFAULT_CONFIG_FILE + " in the engine assets folder" + " (or set " + ROUTER_CONFIG
					+ " in the SMSS to use a different file name).");
		}
		String json = new String(Files.readAllBytes(configFile.toPath()), StandardCharsets.UTF_8);
		RouterConfig cfg = parseAndValidateConfig(json, configFile.getName(), this.engineId);
		applyConfig(cfg);
	}

	/**
	 * Seed the config file from the ROUTER_CONFIG_JSON smss property when the file
	 * does not exist yet - engine creation opens the engine before any asset can be
	 * uploaded. Never overwrites an existing file.
	 */
	private void bootstrapConfigFileIfNeeded(File configFile) throws IOException {
		String bootstrapJson = this.smssProp.getProperty(ROUTER_CONFIG_JSON);
		if (bootstrapJson == null || bootstrapJson.trim().isEmpty()) {
			return;
		}
		File parentFolder = configFile.getParentFile();
		if (parentFolder != null && !parentFolder.exists() && !parentFolder.mkdirs()) {
			throw new IOException(
					"ModelRouterEngine: could not create assets folder " + parentFolder.getAbsolutePath());
		}
		try (Writer writer = new OutputStreamWriter(new FileOutputStream(configFile), StandardCharsets.UTF_8)) {
			writer.write(bootstrapJson.trim());
		}
		classLogger.info("ModelRouterEngine '{}' seeded {} from the {} smss property", this.engineId,
				configFile.getName(), ROUTER_CONFIG_JSON);
	}

	/**
	 * Validates the given JSON, persists it to the config file, and applies it to
	 * the live instance. Nothing is written when validation fails.
	 */
	@Override
	public synchronized void updateConfig(String json) throws IOException {
		File configFile = resolveConfigFile();
		RouterConfig cfg = parseAndValidateConfig(json, configFile.getName(), this.engineId);
		try (Writer writer = new OutputStreamWriter(new FileOutputStream(configFile), StandardCharsets.UTF_8)) {
			writer.write(json);
		}
		applyConfig(cfg);
		classLogger.info("ModelRouterEngine '{}' config updated: {} route(s), mode={}, sticky={}", this.engineId,
				this.routes.size(), this.routingMode, this.sticky);
	}

	/**
	 * Parses and validates router config JSON without touching any engine state, so
	 * both engine open and the update reactor share one set of rules. Fails on
	 * configurations that would otherwise silently degrade at request time (unknown
	 * mode, llm without a classifier or descriptions, weighted without weights, a
	 * route pointing back at the router).
	 *
	 * @param json           the raw config JSON
	 * @param configName     file/source name used in error messages
	 * @param routerEngineId the router's own engine id, for self-reference checks
	 * @return the parsed config, safe to apply
	 */
	public static RouterConfig parseAndValidateConfig(String json, String configName, String routerEngineId) {
		RouterConfig cfg;
		try {
			cfg = new Gson().fromJson(json, RouterConfig.class);
		} catch (JsonSyntaxException e) {
			throw new IllegalArgumentException(
					"ModelRouterEngine: " + configName + " is not valid JSON - " + e.getMessage(), e);
		}
		if (cfg == null || cfg.routes == null || cfg.routes.isEmpty()) {
			throw new IllegalArgumentException("ModelRouterEngine: " + configName + " must define at least one route");
		}

		String mode = resolvedMode(cfg);
		if (!MODE_KEYWORD.equals(mode) && !isClassificationMode(mode) && !MODE_WEIGHTED.equals(mode)) {
			throw new IllegalArgumentException("ModelRouterEngine: unknown mode '" + mode + "' in " + configName
					+ ". Valid modes are: " + MODE_KEYWORD + ", " + MODE_LLM + ", " + MODE_TYPESAFE + ", " + MODE_WEIGHTED);
		}

		Set<String> seenNames = new HashSet<>();
		int totalWeight = 0;
		boolean anyKeywords = false;
		for (int i = 0; i < cfg.routes.size(); i++) {
			RouteConfig rc = cfg.routes.get(i);
			if (rc.engine_id == null || rc.engine_id.trim().isEmpty()) {
				throw new IllegalArgumentException(
						"ModelRouterEngine: route " + i + " in " + configName + " is missing engine_id");
			}
			String name = resolvedRouteName(rc, i);
			if (!seenNames.add(name.toLowerCase())) {
				throw new IllegalArgumentException(
						"ModelRouterEngine: duplicate route name '" + name + "' in " + configName);
			}
			rejectSelfReference(rc.engine_id.trim(), "route '" + name + "'", configName, routerEngineId);
			totalWeight += Math.max(0, rc.weight);
			anyKeywords = anyKeywords || (rc.keywords != null && !rc.keywords.isEmpty());

			if (isClassificationMode(mode) && trimOrNull(rc.description) == null) {
				throw new IllegalArgumentException(
						"ModelRouterEngine: classification mode requires a description on every route - route '" + name + "' in "
								+ configName + " is missing one");
			}
		}

		if (isClassificationMode(mode) && trimOrNull(cfg.classifier_engine) == null) {
			throw new IllegalArgumentException(
					"ModelRouterEngine: classification mode requires classifier_engine in " + configName);
		}
		if (MODE_TYPESAFE.equals(mode) && cfg.routes.size() < 2) {
			throw new IllegalArgumentException("ModelRouterEngine: decision classification requires at least two routes");
		}
		if (MODE_WEIGHTED.equals(mode) && totalWeight <= 0) {
			throw new IllegalArgumentException(
					"ModelRouterEngine: mode 'weighted' requires at least one route with weight > 0 in " + configName);
		}
		if (MODE_KEYWORD.equals(mode) && !anyKeywords) {
			classLogger.warn(
					"ModelRouterEngine ({}): mode is 'keyword' but no route defines keywords - every request will use the default route",
					configName);
		}

		rejectSelfReference(trimOrNull(cfg.default_route), "default_route", configName, routerEngineId);
		rejectSelfReference(trimOrNull(cfg.classifier_engine), "classifier_engine", configName, routerEngineId);
		rejectSelfReference(trimOrNull(cfg.embeddings_engine), "embeddings_engine", configName, routerEngineId);
		if (cfg.fallbacks != null) {
			for (String fallback : cfg.fallbacks) {
				rejectSelfReference(trimOrNull(fallback), "fallbacks", configName, routerEngineId);
			}
		}

		return cfg;
	}

	/** Maps a validated config onto this instance and resets routing state. */
	private void applyConfig(RouterConfig cfg) {
		List<Route> newRoutes = new ArrayList<>();
		for (int i = 0; i < cfg.routes.size(); i++) {
			RouteConfig rc = cfg.routes.get(i);
			List<String> keywords = new ArrayList<>();
			if (rc.keywords != null) {
				for (String kw : rc.keywords) {
					if (kw != null && !kw.trim().isEmpty()) {
						keywords.add(kw.trim().toLowerCase());
					}
				}
			}
			newRoutes.add(new Route(resolvedRouteName(rc, i), rc.engine_id.trim(),
					Collections.unmodifiableList(keywords), Math.max(0, rc.weight), trimOrNull(rc.description)));
		}

		List<String> newFallbacks = new ArrayList<>();
		if (cfg.fallbacks != null) {
			for (String fallback : cfg.fallbacks) {
				String trimmed = trimOrNull(fallback);
				if (trimmed != null) {
					newFallbacks.add(trimmed);
				}
			}
		}

		int newTotalWeight = 0;
		for (Route route : newRoutes) {
			newTotalWeight += route.weight;
		}

		this.routes = Collections.unmodifiableList(newRoutes);
		this.fallbackEngineIds = Collections.unmodifiableList(newFallbacks);
		this.routingMode = resolvedMode(cfg);
		this.sticky = Boolean.TRUE.equals(cfg.sticky);
		this.defaultRouteEngineId = trimOrNull(cfg.default_route);
		this.classifierEngineId = trimOrNull(cfg.classifier_engine);
		this.embeddingsEngineId = trimOrNull(cfg.embeddings_engine);
		this.totalWeight = newTotalWeight;

		this.rrCounter.set(0);
		this.roomRoutePins.clear();
	}

	private static boolean isClassificationMode(String mode) {
		return MODE_LLM.equals(mode) || MODE_TYPESAFE.equals(mode);
	}

	private static String resolvedMode(RouterConfig cfg) {
		return (cfg.mode != null && !cfg.mode.trim().isEmpty()) ? cfg.mode.trim().toLowerCase() : MODE_KEYWORD;
	}

	private static String resolvedRouteName(RouteConfig rc, int index) {
		return (rc.name != null && !rc.name.trim().isEmpty()) ? rc.name.trim() : ("ROUTE_" + index);
	}

	private static void rejectSelfReference(String engineId, String field, String configName, String routerEngineId) {
		if (engineId != null && engineId.equals(routerEngineId)) {
			throw new IllegalArgumentException("ModelRouterEngine: " + field + " in " + configName
					+ " points back at this router (" + routerEngineId + ") - this would recurse forever");
		}
	}

	private static String trimOrNull(String s) {
		return (s != null && !s.trim().isEmpty()) ? s.trim() : null;
	}

	/**
	 * Case-insensitive whole-word alternation over the route's keywords. Word edges
	 * are checked with alphanumeric lookarounds instead of \b so keywords
	 * containing symbols (e.g. "c++") still match.
	 */
	private static Pattern buildKeywordPattern(List<String> keywords) {
		if (keywords == null || keywords.isEmpty()) {
			return null;
		}
		StringBuilder alternation = new StringBuilder();
		for (String kw : keywords) {
			if (alternation.length() > 0) {
				alternation.append("|");
			}
			alternation.append(Pattern.quote(kw));
		}
		return Pattern.compile("(?i)(?<![\\p{Alnum}])(" + alternation + ")(?![\\p{Alnum}])");
	}

	/** Gson DTO for the router JSON file. Field names must match JSON keys. */
	public static class RouterConfig {
		String mode;
		Boolean sticky;
		String default_route;
		List<String> fallbacks;
		String classifier_engine;
		String embeddings_engine;
		List<RouteConfig> routes;
	}

	public static class RouteConfig {
		String name;
		String engine_id;
		int weight;
		List<String> keywords;
		String description;
	}

	// -------------------------------------------------------------------------
	// Core delegation
	// -------------------------------------------------------------------------

	/**
	 * The router's own calls leave the all-models and platform counters and the
	 * team's total to the model they are routed to.
	 */
	@Override
	protected ModelUsageRequest usageRequestFor(ModelUsageRequest request) {
		return request.forRouter();
	}

	/**
	 * The router passes its ceiling on to the model it routes to.
	 */
	@Override
	protected boolean acceptsUsageCeiling() {
		return true;
	}

	@Override
	protected String inferenceLogMessageMethod(String method) {
		// tag this router's own log rows so aggregations and ask-history queries
		// only count the delegated engine's rows
		return "route_" + method;
	}

	/**
	 * The router delegates - its own metadata row does not restrict content.
	 * Enforcement runs in askCall against each routed target's configured input
	 * modalities.
	 */
	@Override
	public void validateInputModalities(List<AbstractMessage> outboundMessages) {
	}

	@Override
	protected AskModelEngineResponse askCall(InputMessage inputMessage, Insight insight, String roomId,
			Map<String, Object> hyperParameters) {

		// Reuses the caller's already-loaded room; the stateless lookup avoids
		// re-acquiring the room mutation lock this request may already hold.
		Room room = RoomUtils.createRoomForStatelessAsk(roomId, insight, this, null);
		User user = insight != null ? insight.getUser() : null;

		Map<String, Object> trace = new LinkedHashMap<>();
		trace.put("trace_id", UUID.randomUUID().toString());
		trace.put("routing_mode", this.routingMode);
		trace.put("classifier_status", "not_used");
		String primaryEngineId = null;
		boolean toolContinuation = inputMessage.hasToolResultPart();
		if ((this.sticky || toolContinuation) && roomId != null) {
			String pinned = roomRoutePins.get(roomId);
			if (pinned != null && userCanUseTarget(user, pinned)) {
				primaryEngineId = pinned;
				trace.put("selection_source", toolContinuation ? "turn" : "sticky");
				trace.put("classifier_status", toolContinuation ? "skipped_turn" : "skipped_sticky");
				classLogger.debug("ModelRouterEngine '{}' room {} reusing pinned engineId={}", this.engineId, roomId,
						pinned);
			}
		}
		if (primaryEngineId == null) {
			InputMessage routingMessage = routingMessage(inputMessage, room);
			String routingText = routingMessage == null ? "" : promptText(routingMessage);
			primaryEngineId = selectRoute(routingText, routingMessage, insight, room, trace);
			if (classLogger.isDebugEnabled()) {
				classLogger.debug("ModelRouterEngine '{}' routing text: {}", this.engineId, truncate(routingText, 200));
			}
		}

		trace.put("selected_engine_id", primaryEngineId);
		String selectedRoute = routeNameForEngine(primaryEngineId);
		if (selectedRoute != null) trace.putIfAbsent("selected_route_name", selectedRoute);
		List<Map<String, Object>> attempts = new ArrayList<>();
		trace.put("attempts", attempts);
		List<String> candidates = buildCandidateList(primaryEngineId);
		Exception lastFailure = null;
		for (String candidateId : candidates) {
			Map<String, Object> attempt = new LinkedHashMap<>();
			attempt.put("engine_id", candidateId);
			attempts.add(attempt);
			if (!userCanUseTarget(user, candidateId)) {
				classLogger.warn(
						"ModelRouterEngine '{}': user does not have access to engineId={} - skipping candidate",
						this.engineId, candidateId);
				attempt.put("status", "access_denied");
				continue;
			}

			IModelEngine targetEngine;
			try {
				targetEngine = resolveEngine(candidateId);
			} catch (Exception e) {
				classLogger.warn("ModelRouterEngine '{}': could not load engineId={} - trying next candidate",
						this.engineId, candidateId, e);
				lastFailure = e;
				attempt.put("status", "unavailable");
				continue;
			}

			classLogger.info("ModelRouterEngine '{}' routing room {} to engineId={}", this.engineId, roomId,
					candidateId);
			try {
				if (targetEngine instanceof AbstractModelEngine targetModel) {
					targetModel.validateInputModalities(room.getMessages(), inputMessage);
				}

				Map<String, Object> params = hyperParameters != null ? new HashMap<>(hyperParameters) : new HashMap<>();
				AskModelEngineResponse response = targetEngine.askRoom(inputMessage, room, params);

				if (roomId != null) {
					roomRoutePins.put(roomId, candidateId);
				}
				attempt.put("status", "success");
				attempt.put("engine_name", targetEngine.getDisplayName());
				trace.put("fallback_used", !candidateId.equals(primaryEngineId));
				attachRouteMetadata(response, candidateId, targetEngine, trace);
				classLogger.info("ModelRouterEngine '{}' trace={} source={} classifierEngine={} classifierStatus={} selected={} served={} ({})",
						this.engineId, trace.get("trace_id"), trace.get("selection_source"), trace.get("classifier_engine_id"),
						trace.get("classifier_status"), primaryEngineId, trace.get("served_engine_id"), trace.get("served_engine_name"));
				if (classLogger.isDebugEnabled()) {
					classLogger.debug("ModelRouterEngine routing trace: {}", new Gson().toJson(trace));
				}
				return response;
			} catch (Exception e) {
				classLogger.warn(
						"ModelRouterEngine '{}': engineId={} failed to serve the request - trying next candidate",
						this.engineId, candidateId, e);
				lastFailure = e;
				attempt.put("status", "failed");
				if (roomId != null) {
					roomRoutePins.remove(roomId);
				}
			}
		}

		classLogger.warn("ModelRouterEngine '{}' trace={}: no destination succeeded; classifier={}, source={}",
				this.engineId, trace.get("trace_id"), trace.get("classifier_status"), trace.get("selection_source"));
		if (classLogger.isDebugEnabled()) classLogger.debug("ModelRouterEngine routing trace: {}", new Gson().toJson(trace));
		if (lastFailure instanceof RuntimeException) {
			throw (RuntimeException) lastFailure;
		}
		if (lastFailure != null) {
			throw new IllegalStateException("ModelRouterEngine: all routing candidates failed", lastFailure);
		}
		throw new IllegalStateException("ModelRouterEngine: user does not have access to any configured route");
	}

	@Override
	protected EmbeddingsModelEngineResponse embeddingsCall(List<String> stringsToEmbed, Insight insight,
			Map<String, Object> parameters) {

		String engId = this.embeddingsEngineId;
		if (engId == null) {
			throw new IllegalStateException("ModelRouterEngine '" + this.engineId
					+ "': no embeddings_engine configured in the router config - this router cannot serve embeddings");
		}
		User user = insight != null ? insight.getUser() : null;
		if (!userCanUseTarget(user, engId)) {
			throw new IllegalStateException("ModelRouterEngine '" + this.engineId
					+ "': user does not have access to the embeddings engine " + engId);
		}
		IModelEngine targetEngine = resolveEngine(engId);
		return targetEngine.embeddings(stringsToEmbed, insight, parameters);
	}

	// -------------------------------------------------------------------------
	// Routing logic
	// -------------------------------------------------------------------------

	/**
	 * The user turn to route on: the resolved input message when the user wrote
	 * it, otherwise the latest user-written input in the room. Tool results are
	 * agent output, not the user's request. Null when no user input has text.
	 */
	private static InputMessage routingMessage(InputMessage inputMessage, Room room) {
		if (isUserInput(inputMessage)) {
			return inputMessage;
		}
		List<AbstractMessage> messages = room.getMessages();
		for (int i = messages.size() - 1; i >= 0; i--) {
			if (messages.get(i) instanceof InputMessage im && isUserInput(im)) {
				return im;
			}
		}
		return null;
	}

	private static boolean isUserInput(InputMessage message) {
		return !message.hasToolResultPart() && !promptText(message).isEmpty();
	}

	/** The UI prompt when present, otherwise the full prompt; trimmed, never null. */
	private static String promptText(InputMessage message) {
		String text = message.getInputUIPrompt();
		if (text == null || text.trim().isEmpty()) {
			text = message.getFullInputPrompt();
		}
		return text == null ? "" : text.trim();
	}

	private String selectRoute(String routingText, InputMessage routingMessage, Insight insight, Room room,
			Map<String, Object> trace) {
		if (MODE_WEIGHTED.equals(this.routingMode)) {
			trace.put("selection_source", "weighted");
			return selectRouteByWeight();
		}
		if (isClassificationMode(this.routingMode)) {
			return selectRouteByLLM(routingText, routingMessage, insight, room, trace);
		}
		return selectRouteByKeyword(routingText, trace);
	}

	/**
	 * Weighted round-robin routing: distributes traffic in strict proportion to the
	 * route weights. A counter cycles 0..total-1 and each route owns a slice. e.g.
	 * weights 30/70 give positions 0-29 to route 0 and 30-99 to route 1, repeating
	 * exactly, so the split is exact over every full cycle.
	 */
	private String selectRouteByWeight() {
		final int total = this.totalWeight;
		if (total <= 0) {
			return fallbackEngineId();
		}
		// Atomically grab the next position in the cycle and wrap at total
		int pos = rrCounter.getAndUpdate(c -> (c + 1) % total);
		int cumulative = 0;
		for (Route r : this.routes) {
			if (r.weight <= 0) {
				continue;
			}
			cumulative += r.weight;
			if (pos < cumulative) {
				classLogger.info("ModelRouterEngine round-robin pos {}/{} -> route '{}' (weight {})", pos, total,
						r.name, r.weight);
				return r.engineId;
			}
		}
		return fallbackEngineId();
	}

	/**
	 * Keyword routing: returns the first route with a whole-word keyword match in
	 * the routing text. Falls back to the default engine.
	 */
	private String selectRouteByKeyword(String routingText, Map<String, Object> trace) {
		for (Route route : this.routes) {
			if (route.keywordPattern == null) {
				continue;
			}
			Matcher matcher = route.keywordPattern.matcher(routingText);
			if (matcher.find()) {
				trace.put("selection_source", trace.containsKey("classifier_engine_id") ? "keyword_fallback" : "keyword");
				trace.put("selected_route_name", route.name);
				classLogger.info("ModelRouterEngine keyword '{}' matched route '{}'", matcher.group(1), route.name);
				return route.engineId;
			}
		}
		trace.put("selection_source", trace.containsKey("classifier_engine_id") ? "default_fallback" : "default");
		classLogger.info("ModelRouterEngine: no keyword matched - using fallback engine");
		return fallbackEngineId();
	}

	/**
	 * Classification routing: chat models return a route name from a prompt, while
	 * TypeSafe decision models answer a structured choice question. Both paths
	 * validate the selected name and fall back to keyword routing on failure.
	 */
	private String selectRouteByLLM(String routingText, InputMessage routingMessage, Insight insight, Room room,
			Map<String, Object> trace) {
		trace.put("classifier_engine_id", this.classifierEngineId);
		long started = System.nanoTime();
		try {
			IModelEngine classifierEngine = resolveEngine(this.classifierEngineId);
			trace.put("classifier_engine_name", classifierEngine.getDisplayName());
			Properties properties = classifierEngine.getSmssProp();
			if (properties != null && properties.getProperty(Constants.MODEL) != null) {
				trace.put("classifier_model", properties.getProperty(Constants.MODEL));
			}
			String routeName;
			if (classifierEngine instanceof ITypeSafeEngine decisionClassifier) {
				if (!MODE_TYPESAFE.equals(this.routingMode)) {
					throw new IllegalArgumentException("TypeSafe classifiers require decision classification mode");
				}
				trace.put("classifier_kind", "typesafe");
				routeName = classifyRouteWithTypeSafe(decisionClassifier, routingText, routingMessage, insight, trace);
			} else {
				if (MODE_TYPESAFE.equals(this.routingMode)) {
					throw new IllegalArgumentException("Decision classification requires a TYPESAFE classifier engine");
				}
				trace.put("classifier_kind", "llm");
				routeName = classifyRouteWithChatModel(classifierEngine, routingText, room, trace);
			}
			routeName = routeName != null ? routeName.trim() : "";

			for (Route route : this.routes) {
				if (route.name.equalsIgnoreCase(routeName)) {
					trace.put("selection_source", "classifier");
					trace.put("classifier_status", "success");
					trace.put("classifier_choice", route.name);
					trace.put("selected_route_name", route.name);
					classLogger.info("ModelRouterEngine: classifier selected route '{}'", routeName);
					return route.engineId;
				}
			}
			trace.put("classifier_status", "invalid_choice");
			trace.put("classifier_error", "Classifier returned a name outside the configured routes");
			classLogger.warn("ModelRouterEngine: classifier returned unknown route '{}', falling back to keyword", routeName);
		} catch (Exception e) {
			trace.put("classifier_status", "error");
			trace.put("classifier_error", e.getClass().getSimpleName());
			classLogger.error("ModelRouterEngine '{}' trace={}: classification failed, falling back to keyword",
					this.engineId, trace.get("trace_id"), e);
		} finally {
			trace.put("classifier_duration_ms", (System.nanoTime() - started) / 1_000_000);
		}
		return selectRouteByKeyword(routingText, trace);
	}

	/**
	 * Sends route descriptions as choice criteria, without creating chat messages.
	 * Images on the routed turn go along when the classifier's configured input
	 * modalities allow them.
	 */
	private String classifyRouteWithTypeSafe(ITypeSafeEngine classifier, String routingText,
			InputMessage routingMessage, Insight insight, Map<String, Object> trace) {
		Map<String, Object> criteria = new LinkedHashMap<>();
		for (Route route : this.routes) {
			criteria.put(route.name, route.description);
		}
		Map<String, Object> question = Map.of("type", "choice",
				"instructions", "Choose the route that best matches the user's request.", "criteria", criteria);
		Map<String, String> images = classifierImages(classifier, routingMessage);
		Object state = routingText;
		if (!images.isEmpty()) {
			// the state refers to each image by its alias, ahead of the text
			List<Object> parts = new ArrayList<>(images.keySet());
			parts.add(routingText);
			state = parts;
		}
		Map<String, Object> request = new LinkedHashMap<>();
		request.put("state", state);
		request.put("questions", Map.of("route", question));
		if (trace.containsKey("classifier_model")) request.put("model", trace.get("classifier_model"));
		trace.put("classifier_request", request);
		TypeSafeModelEngineResponse response = classifier.evaluate(state, Map.of("route", question), images,
				insight, Map.of());
		if (response != null && response.getResponse() != null) {
			Map<String, Object> result = new LinkedHashMap<>();
			for (String key : List.of("model", "answers", "usage")) {
				if (response.getResponse().containsKey(key)) result.put(key, response.getResponse().get(key));
			}
			trace.put("classifier_response", result);
			if (result.get("model") instanceof String model) trace.put("classifier_model", model);
		}
		if (response == null || response.getResponse() == null
				|| !(response.getResponse().get("answers") instanceof Map<?, ?> answers)
				|| !(answers.get("route") instanceof Map<?, ?> answer)
				|| !"choice".equals(answer.get("type")) || !(answer.get("choice") instanceof String choice)
				|| choice.isBlank()) {
			throw new IllegalArgumentException("ModelRouterEngine: decision classifier did not return a route choice");
		}
		return choice;
	}

	/**
	 * Images on the routed turn, keyed by the alias the TypeSafe state refers to
	 * them by. Empty unless the classifier's configured input modalities include
	 * IMAGE: an unconfigured classifier stays text-only, since TypeSafe image input
	 * is opt-in.
	 */
	private static Map<String, String> classifierImages(IModelEngine classifier, InputMessage routingMessage) {
		Map<String, String> images = new LinkedHashMap<>();
		Set<ModelModalityEnum> modalities = classifier.getInputModalities();
		if (routingMessage == null || modalities == null || !modalities.contains(ModelModalityEnum.IMAGE)) {
			return images;
		}
		// shortcut: TypeSafe assets are images with bytes only, so other media and
		// URL images are skipped; pass them once the SDK accepts them
		for (MessageInputMedia media : routingMessage.getMediaInfos()) {
			if (ModelModalityEnum.fromMimeType(media.resolveMimeType()) != ModelModalityEnum.IMAGE) {
				continue;
			}
			String data = media.getBase64Data();
			if (data != null && !data.isBlank()) {
				images.put("<image_" + (images.size() + 1) + ">", data);
			}
		}
		return images;
	}

	/** Retains the existing prompt and chat transport for ordinary LLM classifiers. */
	private String classifyRouteWithChatModel(IModelEngine classifierEngine, String routingText, Room room,
			Map<String, Object> trace) {
		StringBuilder routeList = new StringBuilder();
		for (Route r : this.routes) {
			routeList.append("- ").append(r.name);
			if (r.description != null) {
				routeList.append(": ").append(r.description);
			} else if (!r.keywords.isEmpty()) {
				// defensive only - llm mode validates descriptions at open
				routeList.append(" (for questions about: ").append(String.join(", ", r.keywords)).append(")");
			}
			routeList.append("\n");
		}

		String classificationPrompt = "You are a routing classifier. Given the user question below, "
				+ "reply with ONLY the single route name that best matches - no explanation, no punctuation, no quotes.\n\n"
				+ "Available routes:\n" + routeList + "\nUser question: " + routingText + "\n\nRoute name:";

		Map<String, Object> request = new LinkedHashMap<>();
		request.put("prompt", classificationPrompt);
		if (trace.containsKey("classifier_model")) request.put("model", trace.get("classifier_model"));
		trace.put("classifier_request", request);
		Map<String, Object> params = new HashMap<>();
		InputMessage msg = InputMessage.builder(room).withText(classificationPrompt)
				.withModelType(classifierEngine.getModelType()).withParamMap(params).build();
		params.put(MESSAGE_JSON, MessageUtils.toJsonArrayWithImageData(Arrays.asList(msg)));
		AskModelEngineResponse response = classifierEngine.askRoom(msg, room, params);
		trace.put("classifier_response", response.getStringResponse());
		return response.getStringResponse();
	}

	// -------------------------------------------------------------------------
	// Helpers
	// -------------------------------------------------------------------------

	/**
	 * Ordered, deduplicated failover chain: the selected target first, then the
	 * configured fallbacks, then the default route as last resort.
	 */
	private List<String> buildCandidateList(String primaryEngineId) {
		List<String> candidates = new ArrayList<>();
		addCandidate(candidates, primaryEngineId);
		for (String fallback : this.fallbackEngineIds) {
			addCandidate(candidates, fallback);
		}
		String defaultRoute = this.defaultRouteEngineId;
		List<Route> currentRoutes = this.routes;
		if (defaultRoute != null) {
			addCandidate(candidates, defaultRoute);
		} else if (!currentRoutes.isEmpty()) {
			addCandidate(candidates, currentRoutes.get(0).engineId);
		}
		return candidates;
	}

	private static void addCandidate(List<String> candidates, String engineId) {
		if (engineId != null && !candidates.contains(engineId)) {
			candidates.add(engineId);
		}
	}

	/**
	 * Access to the router does not implicitly grant its backing engines; the
	 * caller must be able to view the target engine. Internal calls without a user
	 * are allowed through.
	 */
	private boolean userCanUseTarget(User user, String engineId) {
		if (user == null) {
			return true;
		}
		try {
			return SecurityEngineUtils.userCanViewEngine(user, engineId);
		} catch (Exception e) {
			classLogger.warn("ModelRouterEngine '{}': access check failed for engineId={} - treating as denied",
					this.engineId, engineId, e);
			return false;
		}
	}

	private void attachRouteMetadata(AskModelEngineResponse<?> response, String targetEngineId, IModelEngine targetEngine,
			Map<String, Object> trace) {
		try {
			Map<String, Object> metadata = response.getMetadata();
			if (metadata == null) {
				metadata = new HashMap<>();
			}
			if (metadata.get(METADATA_ROUTING_TRACE) instanceof Map<?, ?> downstream) {
				trace.put("downstream", downstream);
			}
			metadata.put(METADATA_ROUTING_TRACE, trace);
			metadata.put(METADATA_ROUTER_ENGINE_ID, this.engineId);
			// A nested router already identifies the final serving engine.
			if (!metadata.containsKey(METADATA_ROUTED_ENGINE_ID)) {
				metadata.put(METADATA_ROUTED_ENGINE_ID, targetEngineId);
				metadata.put(METADATA_ROUTED_ENGINE_NAME, targetEngine.getDisplayName());
			}
			metadata.remove(METADATA_ROUTED_ROUTE_NAME);
			trace.put("served_engine_id", metadata.get(METADATA_ROUTED_ENGINE_ID));
			trace.put("served_engine_name", metadata.get(METADATA_ROUTED_ENGINE_NAME));
			String routeName = targetEngineId.equals(trace.get("selected_engine_id"))
					&& trace.get("selected_route_name") instanceof String selected ? selected : routeNameForEngine(targetEngineId);
			if (routeName != null) {
				metadata.put(METADATA_ROUTED_ROUTE_NAME, routeName);
			}
			response.setMetadata(metadata);
		} catch (Exception e) {
			classLogger.debug("ModelRouterEngine '{}': unable to attach route metadata", this.engineId, e);
		}
	}

	private String routeNameForEngine(String engineId) {
		for (Route r : this.routes) {
			if (r.engineId.equals(engineId)) {
				return r.name;
			}
		}
		return null;
	}

	private String fallbackEngineId() {
		String defaultRoute = this.defaultRouteEngineId;
		if (defaultRoute != null) {
			return defaultRoute;
		}
		List<Route> currentRoutes = this.routes;
		if (!currentRoutes.isEmpty()) {
			return currentRoutes.get(0).engineId;
		}
		throw new IllegalStateException("ModelRouterEngine: no routes configured and no default engine set");
	}

	private IModelEngine resolveEngine(String engineId) {
		IEngine engine = Utility.getEngine(engineId);
		if (engine == null) {
			throw new IllegalStateException("ModelRouterEngine: could not load engine with id=" + engineId);
		}
		if (!(engine instanceof IModelEngine)) {
			throw new IllegalStateException("ModelRouterEngine: engine with id=" + engineId
					+ " is not a model engine (found " + engine.getClass().getName() + ")");
		}
		return (IModelEngine) engine;
	}

	private static String truncate(String s, int maxLength) {
		if (s == null || s.length() <= maxLength) {
			return s;
		}
		return s.substring(0, maxLength) + "...";
	}
}
