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
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Reader;
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

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
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
 *     { "name": "code",   "engine_id": "aa876e7e-...", "keywords": ["java", "python", "debug"], "weight": 70 },
 *     { "name": "sports", "engine_id": "8380e91f-...", "keywords": ["nba", "nfl", "score"],     "weight": 30 }
 *   ]
 * }
 * </pre>
 *
 * <h3>Modes</h3>
 * <ul>
 * <li><b>keyword</b> - first route with a whole-word keyword match on the
 * latest user message wins; otherwise the default route.</li>
 * <li><b>llm</b> - the classifier engine is asked to pick a route by name;
 * falls back to keyword matching when classification fails.</li>
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
 */
public class ModelRouterEngine extends AbstractModelEngine {

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
		/** Whole-word matcher over all keywords; null when the route has none. */
		final Pattern keywordPattern;

		Route(String name, String engineId, List<String> keywords, int weight) {
			this.name = name;
			this.engineId = engineId;
			this.keywords = keywords;
			this.weight = weight;
			this.keywordPattern = buildKeywordPattern(keywords);
		}
	}

	private final List<Route> routes = new ArrayList<>();
	private String routingMode = MODE_KEYWORD;
	private boolean sticky = true;
	private String defaultRouteEngineId;
	private final List<String> fallbackEngineIds = new ArrayList<>();
	private String classifierEngineId;
	private String embeddingsEngineId;
	private int totalWeight = 0;
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
	public void close() throws IOException {
		// ModelRouterEngine holds no resources of its own. The backing engines it
		// delegates to are loaded and closed independently by the platform, so there
		// is nothing to tear down here.
	}

	/**
	 * Callers sizing work off this engine (e.g. agent auto-compaction) cannot
	 * know which route will serve them, so answer with the smallest context
	 * window among the serving targets: routes, default route, and fallbacks.
	 * An explicit CONTEXT_WINDOW in the smss/metadata still wins. Targets that
	 * fail to load or do not report a window are skipped; when none report one,
	 * 0 is returned and callers treat it as unknown. Computed once on first use,
	 * so a router reload picks up target changes.
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
		for (Route route : routes) {
			addCandidate(ids, route.engineId);
		}
		addCandidate(ids, this.defaultRouteEngineId);
		for (String fallback : this.fallbackEngineIds) {
			addCandidate(ids, fallback);
		}
		return ids;
	}

	// -------------------------------------------------------------------------
	// Lifecycle
	// -------------------------------------------------------------------------

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		String configFile = this.smssProp.getProperty(ROUTER_CONFIG);
		if (configFile == null || configFile.trim().isEmpty()) {
			configFile = DEFAULT_CONFIG_FILE;
		}
		configFile = configFile.trim();
		bootstrapConfigFileIfNeeded(configFile);
		loadFromJson(configFile);
		validateConfig(configFile);

		classLogger.info("ModelRouterEngine '{}' loaded: {} route(s), mode={}, sticky={}",
				this.engineId, routes.size(), this.routingMode, this.sticky);
	}

	private File resolveConfigFile(String configFile) {
		String assetsFolder = EngineUtility.getSpecificEngineAssetsFolder(
				IEngine.CATALOG_TYPE.MODEL, this.engineId, this.engineName);
		return new File((assetsFolder + "/" + configFile).replace("\\", "/"));
	}

	/**
	 * Seed the config file from the ROUTER_CONFIG_JSON smss property when the
	 * file does not exist yet. Never overwrites an existing file.
	 */
	private void bootstrapConfigFileIfNeeded(String configFile) throws IOException {
		File jsonFile = resolveConfigFile(configFile);
		if (jsonFile.exists()) {
			return;
		}
		String bootstrapJson = this.smssProp.getProperty(ROUTER_CONFIG_JSON);
		if (bootstrapJson == null || bootstrapJson.trim().isEmpty()) {
			return;
		}
		File parentFolder = jsonFile.getParentFile();
		if (parentFolder != null && !parentFolder.exists() && !parentFolder.mkdirs()) {
			throw new IOException("ModelRouterEngine: could not create assets folder " + parentFolder.getAbsolutePath());
		}
		try (FileWriter writer = new FileWriter(jsonFile)) {
			writer.write(bootstrapJson.trim());
		}
		classLogger.info("ModelRouterEngine '{}' seeded {} from the {} smss property",
				this.engineId, jsonFile.getName(), ROUTER_CONFIG_JSON);
	}

	private void loadFromJson(String configFile) throws IOException {
		File jsonFile = resolveConfigFile(configFile);
		if (!jsonFile.exists()) {
			throw new IOException("ModelRouterEngine: routing config not found at " + jsonFile.getAbsolutePath()
					+ ". Place a " + DEFAULT_CONFIG_FILE + " in the engine assets folder"
					+ " (or set " + ROUTER_CONFIG + " in the SMSS to use a different file name).");
		}

		RouterConfig cfg;
		try (Reader reader = new FileReader(jsonFile)) {
			cfg = new Gson().fromJson(reader, RouterConfig.class);
		}
		if (cfg == null || cfg.routes == null || cfg.routes.isEmpty()) {
			throw new IllegalArgumentException("ModelRouterEngine: " + configFile + " must define at least one route");
		}

		for (int i = 0; i < cfg.routes.size(); i++) {
			RouteConfig rc = cfg.routes.get(i);
			if (rc.engine_id == null || rc.engine_id.trim().isEmpty()) {
				throw new IllegalArgumentException("ModelRouterEngine: route " + i + " in " + configFile + " is missing engine_id");
			}
			String name = (rc.name != null && !rc.name.trim().isEmpty()) ? rc.name.trim() : ("ROUTE_" + i);
			List<String> keywords = new ArrayList<>();
			if (rc.keywords != null) {
				for (String kw : rc.keywords) {
					if (kw != null && !kw.trim().isEmpty()) {
						keywords.add(kw.trim().toLowerCase());
					}
				}
			}
			routes.add(new Route(name, rc.engine_id.trim(), keywords, Math.max(0, rc.weight)));
		}

		this.defaultRouteEngineId = trimOrNull(cfg.default_route);
		this.classifierEngineId   = trimOrNull(cfg.classifier_engine);
		this.embeddingsEngineId   = trimOrNull(cfg.embeddings_engine);
		if (cfg.mode != null && !cfg.mode.trim().isEmpty()) {
			this.routingMode = cfg.mode.trim().toLowerCase();
		}
		if (cfg.sticky != null) {
			this.sticky = cfg.sticky.booleanValue();
		}
		if (cfg.fallbacks != null) {
			for (String fb : cfg.fallbacks) {
				String trimmed = trimOrNull(fb);
				if (trimmed != null) {
					this.fallbackEngineIds.add(trimmed);
				}
			}
		}
	}

	/**
	 * Fail engine open on configurations that would otherwise silently degrade
	 * at request time (unknown mode, llm without a classifier, weighted without
	 * weights, a route pointing back at this router).
	 */
	private void validateConfig(String configFile) {
		if (!MODE_KEYWORD.equals(this.routingMode)
				&& !MODE_LLM.equals(this.routingMode)
				&& !MODE_WEIGHTED.equals(this.routingMode)) {
			throw new IllegalArgumentException("ModelRouterEngine: unknown mode '" + this.routingMode
					+ "' in " + configFile + ". Valid modes are: "
					+ MODE_KEYWORD + ", " + MODE_LLM + ", " + MODE_WEIGHTED);
		}

		if (MODE_LLM.equals(this.routingMode) && this.classifierEngineId == null) {
			throw new IllegalArgumentException("ModelRouterEngine: mode 'llm' requires classifier_engine in " + configFile);
		}

		for (Route r : routes) {
			this.totalWeight += r.weight;
		}
		if (MODE_WEIGHTED.equals(this.routingMode) && this.totalWeight <= 0) {
			throw new IllegalArgumentException("ModelRouterEngine: mode 'weighted' requires at least one route with weight > 0 in " + configFile);
		}

		Set<String> seenNames = new HashSet<>();
		for (Route r : routes) {
			if (!seenNames.add(r.name.toLowerCase())) {
				throw new IllegalArgumentException("ModelRouterEngine: duplicate route name '" + r.name + "' in " + configFile);
			}
			rejectSelfReference(r.engineId, "route '" + r.name + "'", configFile);
		}
		rejectSelfReference(this.defaultRouteEngineId, "default_route", configFile);
		rejectSelfReference(this.classifierEngineId, "classifier_engine", configFile);
		rejectSelfReference(this.embeddingsEngineId, "embeddings_engine", configFile);
		for (String fb : this.fallbackEngineIds) {
			rejectSelfReference(fb, "fallbacks", configFile);
		}

		if (MODE_KEYWORD.equals(this.routingMode)) {
			boolean anyKeywords = false;
			for (Route r : routes) {
				anyKeywords = anyKeywords || !r.keywords.isEmpty();
			}
			if (!anyKeywords) {
				classLogger.warn("ModelRouterEngine '{}': mode is 'keyword' but no route defines keywords - every request will use the default route",
						this.engineId);
			}
		}
	}

	private void rejectSelfReference(String engineId, String field, String configFile) {
		if (engineId != null && engineId.equals(this.engineId)) {
			throw new IllegalArgumentException("ModelRouterEngine: " + field + " in " + configFile
					+ " points back at this router (" + this.engineId + ") - this would recurse forever");
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
	private static class RouterConfig {
		String mode;
		Boolean sticky;
		String default_route;
		List<String> fallbacks;
		String classifier_engine;
		String embeddings_engine;
		List<RouteConfig> routes;
	}

	private static class RouteConfig {
		String name;
		String engine_id;
		int weight;
		List<String> keywords;
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

	@Override
	protected AskModelEngineResponse askCall(String question, Object fullPrompt, String context,
			Insight insight, String roomId, Map<String, Object> hyperParameters) {

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
				// Delegate straight to the target's askRoom with the caller's room and a
				// fresh copy of the parameters, so message_json/tools pass through
				// verbatim, provider-specific mutations from a failed attempt do not leak
				// into the next one, and the target's inference log lands under the real
				// room id. Room.ask is skipped on purpose: it would rebuild message_json
				// from scratch and overwrite the room context in the DB when a system
				// prompt is present.
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
					// drop the pin so the next turn re-selects instead of retrying a dead engine
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

		if (this.embeddingsEngineId == null) {
			throw new IllegalStateException("ModelRouterEngine '" + this.engineId
					+ "': no embeddings_engine configured in the router config - this router cannot serve embeddings");
		}
		User user = insight != null ? insight.getUser() : null;
		if (!userCanUseTarget(user, this.embeddingsEngineId)) {
			throw new IllegalStateException("ModelRouterEngine '" + this.engineId
					+ "': user does not have access to the embeddings engine " + this.embeddingsEngineId);
		}
		IModelEngine targetEngine = resolveEngine(this.embeddingsEngineId);
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
		if (this.totalWeight <= 0) {
			return fallbackEngineId();
		}
		// Atomically grab the next position in the cycle and wrap at total
		int pos = rrCounter.getAndUpdate(c -> (c + 1) % this.totalWeight);
		int cumulative = 0;
		for (Route r : routes) {
			if (r.weight <= 0) {
				continue;
			}
			cumulative += r.weight;
			if (pos < cumulative) {
				classLogger.info("ModelRouterEngine round-robin pos {}/{} -> route '{}' (weight {})",
						pos, this.totalWeight, r.name, r.weight);
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
		for (Route route : routes) {
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
			for (Route r : routes) {
				routeList.append("- ").append(r.name);
				if (!r.keywords.isEmpty()) {
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

			// One-shot call against the caller's room and insight: askRoom is invoked
			// directly (never Room.ask) so nothing is appended to the room history and
			// no per-request classification rooms are written to the logs database.
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

			for (Route route : routes) {
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
		for (String fb : this.fallbackEngineIds) {
			addCandidate(candidates, fb);
		}
		if (this.defaultRouteEngineId != null) {
			addCandidate(candidates, this.defaultRouteEngineId);
		} else if (!routes.isEmpty()) {
			addCandidate(candidates, routes.get(0).engineId);
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
		for (Route r : routes) {
			if (r.engineId.equals(engineId)) {
				return r.name;
			}
		}
		return null;
	}

	private String fallbackEngineId() {
		if (this.defaultRouteEngineId != null) {
			return this.defaultRouteEngineId;
		}
		if (!routes.isEmpty()) {
			return routes.get(0).engineId;
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
