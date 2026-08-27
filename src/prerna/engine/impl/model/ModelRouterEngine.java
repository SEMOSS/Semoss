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
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.MessageUtils;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.om.Insight;
import prerna.util.EngineUtility;
import prerna.util.Utility;

/**
 * ModelRouterEngine is a routing {@link IModelEngine} that dispatches each
 * request to one of several backing model engines.
 *
 * <p>All routing configuration lives in a JSON file in the engine's assets
 * folder. By convention the engine loads <b>router.json</b>; the optional SMSS
 * property ROUTER_CONFIG overrides the file name. Beyond that, the SMSS file
 * only needs the standard engine identity keys:
 * <pre>
 * ENGINE_TYPE = prerna.engine.impl.model.ModelRouterEngine
 * MODEL_TYPE  = MODEL_ROUTER
 * </pre>
 *
 * <h3>router.json schema</h3>
 * <pre>
 * {
 *   "mode": "keyword",                 // "keyword" | "llm" | "weighted"
 *   "sticky": true,                    // pin a conversation to the route that first serves it (default true)
 *   "default_route": "&lt;engineId&gt;",     // used when no route matches (defaults to route 0)
 *   "fallbacks": ["&lt;engineId&gt;"],       // tried in order when the chosen target fails
 *   "classifier_engine": "&lt;engineId&gt;", // required for "llm" mode
 *   "embeddings_engine": "&lt;engineId&gt;", // required for the router to serve embeddings
 *   "routes": [
 *     { "name": "code",   "engine_id": "aa876e7e-...", "keywords": ["java", "python", "debug"],
 *       "description": "Programming questions: debugging, writing and reviewing code", "weight": 70 },
 *     { "name": "sports", "engine_id": "8380e91f-...", "keywords": ["nba", "nfl", "score"],
 *       "description": "Sports questions: scores, players, teams and schedules", "weight": 30 }
 *   ]
 * }
 * </pre>
 *
 * <h3>Modes</h3>
 * <ul>
 * <li><b>keyword</b> - first route with a whole-word keyword match on the
 * latest user message wins; otherwise the default route.</li>
 * <li><b>llm</b> - the classifier engine picks a route by reading each route's
 * description (required on every route in this mode); falls back to keyword
 * matching when classification fails, so keywords are the optional safety
 * net here.</li>
 * <li><b>weighted</b> - deterministic weighted round-robin across routes with
 * weight &gt; 0 (weights 30/70 give an exact 30/70 split every cycle).</li>
 * </ul>
 *
 * <h3>Sticky routing</h3>
 * When sticky is on (the default), the first turn of a room selects a route
 * and later turns reuse it, so a conversation stays on one model and llm mode
 * pays the classifier cost only once per room. Under weighted mode this makes
 * the traffic split per-conversation rather than per-request. A pin is dropped
 * when its engine fails and the turn is served by a failover candidate. Pins
 * are held in a bounded in-memory map per router instance, so they reset on
 * engine reload and are not shared across nodes.
 *
 * <h3>Failover</h3>
 * When the chosen target fails (engine will not load, or the ask errors), the
 * router tries the fallbacks list in order and finally the default route. The
 * last failure is rethrown when every candidate fails.
 *
 * <h3>Access control</h3>
 * Access to the router does NOT implicitly grant its backing engines: each
 * candidate target (ask and embeddings) is checked with
 * {@link SecurityEngineUtils#userCanViewEngine(User, String)} for the calling
 * user, and denied candidates are skipped. The classifier engine is exempt -
 * it is internal plumbing whose output the user never sees directly.
 *
 * <h3>Inference logs</h3>
 * Requests are logged twice by design: once under this router's engine id
 * (user-facing attribution) and once under the delegated engine's id (actual
 * model usage). The router's rows are tagged with messageMethod "route_ask" /
 * "route_embeddings" so ask-history queries and usage aggregations only count
 * the delegated engine's "ask" / "embeddings" rows.
 *
 * <p>The chosen target is surfaced on the response metadata under the
 * router_engine_id / routed_engine_id / routed_route_name keys.
 *
 * <p>The configuration can be edited at runtime through the
 * GetModelRouterConfig / UpdateModelRouterConfig reactors, which read and
 * rewrite the config file and then {@link #reloadConfig()} the live instance.
 */
public class ModelRouterEngine extends AbstractModelEngine implements IModelRouterEngine {

	private static final Logger classLogger = LogManager.getLogger(ModelRouterEngine.class);

	/** Optional SMSS property overriding the config file name in the assets folder. */
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
	public static final String METADATA_ROUTED_ROUTE_NAME = "routed_route_name";

	private static final String MODE_KEYWORD  = "keyword";
	private static final String MODE_LLM      = "llm";
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
	private volatile boolean sticky = true;
	private volatile String defaultRouteEngineId;
	private volatile List<String> fallbackEngineIds = Collections.emptyList();
	private volatile String classifierEngineId;
	private volatile String embeddingsEngineId;
	private volatile int totalWeight = 0;
	/** Round-robin counter for weighted mode - increments on every weighted call. */
	private final AtomicInteger rrCounter = new AtomicInteger(0);
	/** Lazily computed min context window across serving targets; null = not yet computed. */
	private volatile Integer derivedContextWindow;
	/** LRU of roomId -> engineId that last served the room, used when sticky is on. */
	private final Map<String, String> roomRoutePins = Collections.synchronizedMap(
			new LinkedHashMap<String, String>(128, 0.75f, true) {
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
	public void close() throws IOException {}

	/**
	 * Callers sizing work off this engine (e.g. agent auto-compaction) cannot
	 * know which route will serve them, so answer with the smallest context
	 * window among the serving targets: routes, default route, and fallbacks.
	 * An explicit CONTEXT_WINDOW in the smss/metadata still wins. Targets that
	 * fail to load or do not report a window are skipped; when none report one,
	 * 0 is returned and callers treat it as unknown. Computed once per config
	 * (re)load.
	 */
	@Override
	public int getContextWindow() {
		int inherited = super.getContextWindow();
		if (inherited > 0) {
			return inherited;
		}
		Integer derived = this.derivedContextWindow;
		if (derived == null) {
			derived = computeMinTargetContextWindow();
			this.derivedContextWindow = derived;
		}
		return derived.intValue();
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
		classLogger.info("ModelRouterEngine '{}' loaded: {} route(s), mode={}, sticky={}",
				this.engineId, this.routes.size(), this.routingMode, this.sticky);
	}

	/** The config file this router reads: assets/&lt;ROUTER_CONFIG or router.json&gt;. */
	public File resolveConfigFile() {
		String configFileName = this.smssProp.getProperty(ROUTER_CONFIG);
		if (configFileName == null || configFileName.trim().isEmpty()) {
			configFileName = DEFAULT_CONFIG_FILE;
		}
		String assetsFolder = EngineUtility.getSpecificEngineAssetsFolder(
				IEngine.CATALOG_TYPE.MODEL, this.engineId, this.engineName);
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
	 * Re-reads and applies the config file on the live instance. Used at open
	 * and after {@link #updateConfig(String)} rewrites the file.
	 */
	@Override
	public synchronized void reloadConfig() throws IOException {
		File configFile = resolveConfigFile();
		if (!configFile.exists()) {
			bootstrapConfigFileIfNeeded(configFile);
		}
		if (!configFile.exists()) {
			throw new IOException("ModelRouterEngine: routing config not found at " + configFile.getAbsolutePath()
					+ ". Place a " + DEFAULT_CONFIG_FILE + " in the engine assets folder"
					+ " (or set " + ROUTER_CONFIG + " in the SMSS to use a different file name).");
		}
		String json = new String(Files.readAllBytes(configFile.toPath()), StandardCharsets.UTF_8);
		RouterConfig cfg = parseAndValidateConfig(json, configFile.getName(), this.engineId);
		applyConfig(cfg);
	}

	/**
	 * Seed the config file from the ROUTER_CONFIG_JSON smss property when the
	 * file does not exist yet - engine creation opens the engine before any
	 * asset can be uploaded. Never overwrites an existing file.
	 */
	private void bootstrapConfigFileIfNeeded(File configFile) throws IOException {
		String bootstrapJson = this.smssProp.getProperty(ROUTER_CONFIG_JSON);
		if (bootstrapJson == null || bootstrapJson.trim().isEmpty()) {
			return;
		}
		File parentFolder = configFile.getParentFile();
		if (parentFolder != null && !parentFolder.exists() && !parentFolder.mkdirs()) {
			throw new IOException("ModelRouterEngine: could not create assets folder " + parentFolder.getAbsolutePath());
		}
		try (Writer writer = new OutputStreamWriter(new FileOutputStream(configFile), StandardCharsets.UTF_8)) {
			writer.write(bootstrapJson.trim());
		}
		classLogger.info("ModelRouterEngine '{}' seeded {} from the {} smss property",
				this.engineId, configFile.getName(), ROUTER_CONFIG_JSON);
	}

	/**
	 * Validates the given JSON, persists it to the config file, and applies it
	 * to the live instance. Nothing is written when validation fails.
	 */
	@Override
	public synchronized void updateConfig(String json) throws IOException {
		File configFile = resolveConfigFile();
		RouterConfig cfg = parseAndValidateConfig(json, configFile.getName(), this.engineId);
		try (Writer writer = new OutputStreamWriter(new FileOutputStream(configFile), StandardCharsets.UTF_8)) {
			writer.write(json);
		}
		applyConfig(cfg);
		classLogger.info("ModelRouterEngine '{}' config updated: {} route(s), mode={}, sticky={}",
				this.engineId, this.routes.size(), this.routingMode, this.sticky);
	}

	/**
	 * Parses and validates router config JSON without touching any engine
	 * state, so both engine open and the update reactor share one set of
	 * rules. Fails on configurations that would otherwise silently degrade at
	 * request time (unknown mode, llm without a classifier or descriptions,
	 * weighted without weights, a route pointing back at the router).
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
			throw new IllegalArgumentException("ModelRouterEngine: " + configName + " is not valid JSON - " + e.getMessage(), e);
		}
		if (cfg == null || cfg.routes == null || cfg.routes.isEmpty()) {
			throw new IllegalArgumentException("ModelRouterEngine: " + configName + " must define at least one route");
		}

		String mode = resolvedMode(cfg);
		if (!MODE_KEYWORD.equals(mode) && !MODE_LLM.equals(mode) && !MODE_WEIGHTED.equals(mode)) {
			throw new IllegalArgumentException("ModelRouterEngine: unknown mode '" + mode + "' in " + configName
					+ ". Valid modes are: " + MODE_KEYWORD + ", " + MODE_LLM + ", " + MODE_WEIGHTED);
		}

		Set<String> seenNames = new HashSet<>();
		int totalWeight = 0;
		boolean anyKeywords = false;
		for (int i = 0; i < cfg.routes.size(); i++) {
			RouteConfig rc = cfg.routes.get(i);
			if (rc.engine_id == null || rc.engine_id.trim().isEmpty()) {
				throw new IllegalArgumentException("ModelRouterEngine: route " + i + " in " + configName + " is missing engine_id");
			}
			String name = resolvedRouteName(rc, i);
			if (!seenNames.add(name.toLowerCase())) {
				throw new IllegalArgumentException("ModelRouterEngine: duplicate route name '" + name + "' in " + configName);
			}
			rejectSelfReference(rc.engine_id.trim(), "route '" + name + "'", configName, routerEngineId);
			totalWeight += Math.max(0, rc.weight);
			anyKeywords = anyKeywords || (rc.keywords != null && !rc.keywords.isEmpty());

			if (MODE_LLM.equals(mode) && trimOrNull(rc.description) == null) {
				throw new IllegalArgumentException("ModelRouterEngine: mode 'llm' requires a description on every route - route '"
						+ name + "' in " + configName + " is missing one");
			}
		}

		if (MODE_LLM.equals(mode) && trimOrNull(cfg.classifier_engine) == null) {
			throw new IllegalArgumentException("ModelRouterEngine: mode 'llm' requires classifier_engine in " + configName);
		}
		if (MODE_WEIGHTED.equals(mode) && totalWeight <= 0) {
			throw new IllegalArgumentException("ModelRouterEngine: mode 'weighted' requires at least one route with weight > 0 in " + configName);
		}
		if (MODE_KEYWORD.equals(mode) && !anyKeywords) {
			classLogger.warn("ModelRouterEngine ({}): mode is 'keyword' but no route defines keywords - every request will use the default route",
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
		this.sticky = cfg.sticky == null || cfg.sticky.booleanValue();
		this.defaultRouteEngineId = trimOrNull(cfg.default_route);
		this.classifierEngineId = trimOrNull(cfg.classifier_engine);
		this.embeddingsEngineId = trimOrNull(cfg.embeddings_engine);
		this.totalWeight = newTotalWeight;

		this.rrCounter.set(0);
		this.roomRoutePins.clear();
		this.derivedContextWindow = null;
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
	 * Case-insensitive whole-word alternation over the route's keywords. Word
	 * edges are checked with alphanumeric lookarounds instead of \b so keywords
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

	@Override
	protected String inferenceLogMessageMethod(String method) {
		// tag this router's own log rows so aggregations and ask-history queries
		// only count the delegated engine's rows
		return "route_" + method;
	}

	/**
	 * Internal parameter key handing the caller's real input message from
	 * askRoom to askCall so each routing candidate can be validated against the
	 * actual conversation content. Removed before anything reaches a provider.
	 */
	private static final String ROUTED_INPUT_MESSAGE = "__routedInputMessage";

	@Override
	public AskModelEngineResponse askRoom(String question, Room room, AbstractMessage inputMessage,
			Map<String, Object> parameters) {
		if (parameters != null && inputMessage != null) {
			parameters.put(ROUTED_INPUT_MESSAGE, inputMessage);
		}
		try {
			return super.askRoom(question, room, inputMessage, parameters);
		} finally {
			// never let the stash outlive the call - the caller's map may be
			// reused or serialized
			if (parameters != null) {
				parameters.remove(ROUTED_INPUT_MESSAGE);
			}
		}
	}

	/**
	 * The router delegates - its own metadata row does not restrict content.
	 * Enforcement runs in askCall against each routed target's configured
	 * input modalities.
	 */
	@Override
	public void validateInputModalities(List<AbstractMessage> outboundMessages) {
	}

	@Override
	protected AskModelEngineResponse askCall(String question, Object fullPrompt, String context,
			Insight insight, String roomId, Map<String, Object> hyperParameters) {

		AbstractMessage routedInputMessage = null;
		if (hyperParameters != null
				&& hyperParameters.remove(ROUTED_INPUT_MESSAGE) instanceof AbstractMessage stashed) {
			routedInputMessage = stashed;
		}

		// Reuses the caller's already-loaded room; the stateless lookup avoids
		// re-acquiring the room mutation lock this request may already hold.
		Room room = RoomUtils.createRoomForStatelessAsk(roomId, insight, this, null);
		User user = insight != null ? insight.getUser() : null;

		String primaryEngineId = null;
		if (this.sticky && roomId != null) {
			String pinned = roomRoutePins.get(roomId);
			if (pinned != null && userCanUseTarget(user, pinned)) {
				primaryEngineId = pinned;
				classLogger.debug("ModelRouterEngine '{}' room {} reusing pinned engineId={}",
						this.engineId, roomId, pinned);
			}
		}
		if (primaryEngineId == null) {
			String routingText = extractRoutingText(question, room);
			primaryEngineId = selectRoute(routingText, insight, room);
			if (classLogger.isDebugEnabled()) {
				classLogger.debug("ModelRouterEngine '{}' routing text: {}",
						this.engineId, truncate(routingText, 200));
			}
		}

		List<String> candidates = buildCandidateList(primaryEngineId);
		Exception lastFailure = null;
		for (String candidateId : candidates) {
			if (!userCanUseTarget(user, candidateId)) {
				classLogger.warn("ModelRouterEngine '{}': user does not have access to engineId={} - skipping candidate",
						this.engineId, candidateId);
				continue;
			}

			IModelEngine targetEngine;
			try {
				targetEngine = resolveEngine(candidateId);
			} catch (Exception e) {
				classLogger.warn("ModelRouterEngine '{}': could not load engineId={} - trying next candidate",
						this.engineId, candidateId, e);
				lastFailure = e;
				continue;
			}

			classLogger.info("ModelRouterEngine '{}' routing room {} to engineId={}",
					this.engineId, roomId, candidateId);
			try {
				if (targetEngine instanceof AbstractModelEngine targetModel) {
					if (routedInputMessage != null) {
						targetModel.validateInputModalities(room.getMessages(), routedInputMessage);
					} else {
						targetModel.validateInputModalities(room.getMessages());
					}
				}

				Map<String, Object> params = hyperParameters != null
						? new HashMap<>(hyperParameters)
						: new HashMap<>();
				InputMessage msg = InputMessage.builder(room)
						.withSystemPrompt(context)
						.withText(question)
						.withModelType(targetEngine.getModelType())
						.withParamMap(params)
						.build();
				AskModelEngineResponse response = targetEngine.askRoom(question, room, msg, params);

				if (this.sticky && roomId != null) {
					roomRoutePins.put(roomId, candidateId);
				}
				attachRouteMetadata(response, candidateId);
				return response;
			} catch (Exception e) {
				classLogger.warn("ModelRouterEngine '{}': engineId={} failed to serve the request - trying next candidate",
						this.engineId, candidateId, e);
				lastFailure = e;
				if (this.sticky && roomId != null) {
					roomRoutePins.remove(roomId);
				}
			}
		}

		if (lastFailure instanceof RuntimeException) {
			throw (RuntimeException) lastFailure;
		}
		if (lastFailure != null) {
			throw new IllegalStateException("ModelRouterEngine: all routing candidates failed", lastFailure);
		}
		throw new IllegalStateException("ModelRouterEngine: user does not have access to any configured route");
	}

	@Override
	protected EmbeddingsModelEngineResponse embeddingsCall(List<String> stringsToEmbed,
			Insight insight, Map<String, Object> parameters) {

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
	 * On the full-prompt path askCall receives the serialized conversation JSON
	 * as the question; routing on that blob would match keywords in the system
	 * prompt, tool definitions, and stale turns. Pull the latest user-authored
	 * message off the room instead, and fall back to the raw question text.
	 */
	private static String extractRoutingText(String question, Room room) {
		String raw = question != null ? question.trim() : "";
		if (!raw.startsWith("[")) {
			// plain-question path: askCall already received the user's text
			return raw;
		}
		List<AbstractMessage> messages = room.getMessages();
		for (int i = messages.size() - 1; i >= 0; i--) {
			if (messages.get(i) instanceof InputMessage) {
				InputMessage im = (InputMessage) messages.get(i);
				// the UI prompt is only set on user-authored turns, which skips
				// tool-result input messages in agent loops
				String text = im.getInputUIPrompt();
				if (text == null || text.trim().isEmpty()) {
					text = im.getInputPrompt();
				}
				if (text != null && !text.trim().isEmpty()) {
					return text.trim();
				}
			}
		}
		return raw;
	}

	private String selectRoute(String routingText, Insight insight, Room room) {
		if (MODE_WEIGHTED.equals(this.routingMode)) {
			return selectRouteByWeight();
		}
		if (MODE_LLM.equals(this.routingMode)) {
			return selectRouteByLLM(routingText, insight, room);
		}
		return selectRouteByKeyword(routingText);
	}

	/**
	 * Weighted round-robin routing: distributes traffic in strict proportion to
	 * the route weights. A counter cycles 0..total-1 and each route owns a slice.
	 * e.g. weights 30/70 give positions 0-29 to route 0 and 30-99 to route 1,
	 * repeating exactly, so the split is exact over every full cycle.
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
				classLogger.info("ModelRouterEngine round-robin pos {}/{} -> route '{}' (weight {})",
						pos, total, r.name, r.weight);
				return r.engineId;
			}
		}
		return fallbackEngineId();
	}

	/**
	 * Keyword routing: returns the first route with a whole-word keyword match
	 * in the routing text. Falls back to the default engine.
	 */
	private String selectRouteByKeyword(String routingText) {
		for (Route route : this.routes) {
			if (route.keywordPattern == null) {
				continue;
			}
			Matcher matcher = route.keywordPattern.matcher(routingText);
			if (matcher.find()) {
				classLogger.info("ModelRouterEngine keyword '{}' matched route '{}'", matcher.group(1), route.name);
				return route.engineId;
			}
		}
		classLogger.info("ModelRouterEngine: no keyword matched - using fallback engine");
		return fallbackEngineId();
	}

	/**
	 * LLM routing: sends a compact classification prompt to the classifier engine,
	 * expects exactly one route name back, then resolves it. Gracefully degrades to
	 * keyword routing if the LLM call fails or returns an unrecognised name.
	 */
	private String selectRouteByLLM(String routingText, Insight insight, Room room) {
		try {
			StringBuilder routeList = new StringBuilder();
			for (Route r : this.routes) {
				routeList.append("- ").append(r.name);
				if (r.description != null) {
					routeList.append(": ").append(r.description);
				} else if (!r.keywords.isEmpty()) {
					// defensive only - llm mode validates descriptions at open
					routeList.append(" (for questions about: ")
							.append(String.join(", ", r.keywords))
							.append(")");
				}
				routeList.append("\n");
			}

			String classificationPrompt =
					"You are a routing classifier. Given the user question below, "
					+ "reply with ONLY the single route name that best matches - no explanation, no punctuation, no quotes.\n\n"
					+ "Available routes:\n" + routeList
					+ "\nUser question: " + routingText
					+ "\n\nRoute name:";

			IModelEngine classifierEngine = resolveEngine(this.classifierEngineId);

			Map<String, Object> params = new HashMap<>();
			InputMessage msg = InputMessage.builder(room)
					.withText(classificationPrompt)
					.withModelType(classifierEngine.getModelType())
					.withParamMap(params)
					.build();
			params.put(MESSAGE_JSON, MessageUtils.toJsonArrayWithImageData(Arrays.asList(msg)));
			AskModelEngineResponse response = classifierEngine.askRoom(classificationPrompt, room, msg, params);

			String routeName = response.getStringResponse();
			routeName = routeName != null ? routeName.trim() : "";

			for (Route route : this.routes) {
				if (route.name.equalsIgnoreCase(routeName)) {
					classLogger.info("ModelRouterEngine: LLM classified question as route '{}'", routeName);
					return route.engineId;
				}
			}
			classLogger.warn("ModelRouterEngine: LLM returned unknown route '{}', falling back to keyword", routeName);
		} catch (Exception e) {
			classLogger.error("ModelRouterEngine: LLM classification failed, falling back to keyword", e);
		}
		return selectRouteByKeyword(routingText);
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
	 * caller must be able to view the target engine. Internal calls without a
	 * user are allowed through.
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

	private void attachRouteMetadata(AskModelEngineResponse<?> response, String targetEngineId) {
		try {
			Map<String, Object> metadata = response.getMetadata();
			if (metadata == null) {
				metadata = new HashMap<>();
			}
			metadata.put(METADATA_ROUTER_ENGINE_ID, this.engineId);
			metadata.put(METADATA_ROUTED_ENGINE_ID, targetEngineId);
			String routeName = routeNameForEngine(targetEngineId);
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
