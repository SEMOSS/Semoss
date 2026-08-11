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
import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.api.ModelTypeEnum;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.engine.impl.model.responses.AskModelEngineResponse;
import prerna.engine.impl.model.responses.EmbeddingsModelEngineResponse;
import prerna.om.Insight;
import prerna.om.InsightStore;
import prerna.util.EngineUtility;
import prerna.util.Utility;

/**
 * ModelRouterEngine is a routing {@link IModelEngine} that dispatches each
 * query to one of several backing engines based on keyword matching or
 * LLM-based classification. It extends {@link AbstractModelEngine} so all
 * standard inference-logging and usage-restriction logic continues to apply
 * at the ModelRouterEngine level.
 *
 * <h3>SMSS configuration keys</h3>
 * <pre>
 * ENGINE_TYPE             = prerna.engine.impl.model.ModelRouterEngine
 * MODEL_TYPE              = MODEL_ROUTER
 *
 * # Number of routes (required)
 * ROUTE_COUNT             = 2
 *
 * # Route 0 — sports queries go to GPT
 * ROUTE_0_NAME            = sports
 * ROUTE_0_ENGINE_ID       = 8380e91f-7c0b-46a1-ad2a-d795b24037b5
 * ROUTE_0_KEYWORDS        = sports,nba,nfl,score,player,game,match,tournament
 *
 * # Route 1 — code/weather queries go to Claude
 * ROUTE_1_NAME            = code
 * ROUTE_1_ENGINE_ID       = aa876e7e-e78e-404d-b7db-1a44236bc2a5
 * ROUTE_1_KEYWORDS        = code,python,java,function,debug,error,weather,forecast
 *
 * # Fallback when no keyword matches (defaults to ROUTE_0 if omitted)
 * DEFAULT_ROUTE_ENGINE_ID = aa876e7e-e78e-404d-b7db-1a44236bc2a5
 *
 * # "keyword" (default) or "llm" — LLM mode calls CLASSIFIER_ENGINE_ID first
 * CLASSIFIER_MODE         = keyword
 *
 * # Only needed when CLASSIFIER_MODE = llm
 * CLASSIFIER_ENGINE_ID    = aa876e7e-e78e-404d-b7db-1a44236bc2a5
 *
 * # Engine used for embeddings delegation (falls back to DEFAULT_ROUTE_ENGINE_ID)
 * EMBEDDINGS_ENGINE_ID    = aa876e7e-e78e-404d-b7db-1a44236bc2a5
 * </pre>
 */
public class ModelRouterEngine extends AbstractModelEngine {

	private static final Logger classLogger = LogManager.getLogger(ModelRouterEngine.class);

	// SMSS property key constants
	public static final String ROUTER_CONFIG           = "ROUTER_CONFIG";
	public static final String ROUTE_COUNT             = "ROUTE_COUNT";
	public static final String ROUTE_NAME_SUFFIX       = "_NAME";
	public static final String ROUTE_ENGINE_SUFFIX     = "_ENGINE_ID";
	public static final String ROUTE_KEYWORDS_SUFFIX   = "_KEYWORDS";
	public static final String ROUTE_WEIGHT_SUFFIX     = "_WEIGHT";
	public static final String DEFAULT_ROUTE_ENGINE_ID = "DEFAULT_ROUTE_ENGINE_ID";
	public static final String CLASSIFIER_MODE         = "CLASSIFIER_MODE";
	public static final String CLASSIFIER_ENGINE_ID    = "CLASSIFIER_ENGINE_ID";
	public static final String EMBEDDINGS_ENGINE_ID    = "EMBEDDINGS_ENGINE_ID";

	private static final String MODE_KEYWORD  = "keyword";
	private static final String MODE_LLM      = "llm";
	private static final String MODE_WEIGHTED = "weighted";

	// -------------------------------------------------------------------------
	// Internal route descriptor
	// -------------------------------------------------------------------------
	private static class Route {
		final String name;
		final String engineId;
		final List<String> keywords;
		final int weight;

		Route(String name, String engineId, List<String> keywords, int weight) {
			this.name = name;
			this.engineId = engineId;
			this.keywords = keywords;
			this.weight = weight;
		}
	}

	private final List<Route> routes = new ArrayList<>();
	private String defaultRouteEngineId;
	private String classifierMode = MODE_KEYWORD;
	private String classifierEngineId;
	private String embeddingsEngineId;
	/** Round-robin counter for weighted mode — increments on every weighted call. */
	private final AtomicInteger rrCounter = new AtomicInteger(0);

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

	// -------------------------------------------------------------------------
	// Lifecycle
	// -------------------------------------------------------------------------

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		String configFile = this.smssProp.getProperty(ROUTER_CONFIG);
		if (configFile != null && !configFile.trim().isEmpty()) {
			loadFromJson(configFile.trim());
		} else {
			loadFromProperties();
		}

		classLogger.info("ModelRouterEngine '{}' loaded: {} route(s), classifierMode={}",
				this.engineId, routes.size(), this.classifierMode);
	}

	/**
	 * Load router config from a JSON file in the engine's assets folder (Portkey-style).
	 * Schema:
	 *   {
	 *     "mode": "weighted|llm|keyword",
	 *     "default_route": "<engineId>",
	 *     "classifier_engine": "<engineId>",
	 *     "embeddings_engine": "<engineId>",
	 *     "routes": [
	 *       { "name": "claude", "engine_id": "abc...", "weight": 30, "keywords": ["code","debug"] }
	 *     ]
	 *   }
	 */
	private void loadFromJson(String configFile) throws IOException {
		String assetsFolder = EngineUtility.getSpecificEngineAssetsFolder(
				IEngine.CATALOG_TYPE.MODEL, this.engineId, this.engineName);
		File jsonFile = new File((assetsFolder + "/" + configFile).replace("\\", "/"));
		if (!jsonFile.exists()) {
			throw new IOException("ModelRouterEngine: " + ROUTER_CONFIG + " file not found at " + jsonFile.getAbsolutePath());
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
					if (kw != null && !kw.trim().isEmpty()) keywords.add(kw.trim().toLowerCase());
				}
			}
			routes.add(new Route(name, rc.engine_id.trim(), keywords, Math.max(0, rc.weight)));
		}

		this.defaultRouteEngineId = trimOrNull(cfg.default_route);
		this.classifierEngineId   = trimOrNull(cfg.classifier_engine);
		this.embeddingsEngineId   = trimOrNull(cfg.embeddings_engine);
		if (cfg.mode != null && !cfg.mode.trim().isEmpty()) {
			this.classifierMode = cfg.mode.trim().toLowerCase();
		}
		classLogger.info("[ModelRouter] Loaded config from {}", jsonFile.getName());
	}

	/**
	 * Legacy loader: parse ROUTE_x_* properties directly from the SMSS. Kept for
	 * backward compatibility with SMSS files that don't define ROUTER_CONFIG.
	 */
	private void loadFromProperties() {
		String routeCountStr = this.smssProp.getProperty(ROUTE_COUNT);
		if (routeCountStr == null || routeCountStr.trim().isEmpty()) {
			throw new IllegalArgumentException("ModelRouterEngine requires either " + ROUTER_CONFIG + " or " + ROUTE_COUNT + " in its SMSS file");
		}

		int routeCount;
		try {
			routeCount = Integer.parseInt(routeCountStr.trim());
		} catch (NumberFormatException e) {
			throw new IllegalArgumentException("ModelRouterEngine: " + ROUTE_COUNT + " must be an integer, got: " + routeCountStr);
		}

		for (int i = 0; i < routeCount; i++) {
			String prefix      = "ROUTE_" + i;
			String name        = this.smssProp.getProperty(prefix + ROUTE_NAME_SUFFIX, prefix);
			String engineId    = this.smssProp.getProperty(prefix + ROUTE_ENGINE_SUFFIX);
			String keywordsRaw = this.smssProp.getProperty(prefix + ROUTE_KEYWORDS_SUFFIX, "");
			String weightRaw   = this.smssProp.getProperty(prefix + ROUTE_WEIGHT_SUFFIX, "0");

			if (engineId == null || engineId.trim().isEmpty()) {
				throw new IllegalArgumentException("ModelRouterEngine: route " + i + " is missing " + prefix + ROUTE_ENGINE_SUFFIX);
			}

			int weight = 0;
			try {
				weight = Integer.parseInt(weightRaw.trim());
			} catch (NumberFormatException e) {
				classLogger.warn("ModelRouterEngine: route {} has non-integer {}{} = '{}', defaulting weight to 0",
						i, prefix, ROUTE_WEIGHT_SUFFIX, weightRaw);
			}

			List<String> keywords = new ArrayList<>();
			for (String kw : keywordsRaw.split(",")) {
				String trimmed = kw.trim().toLowerCase();
				if (!trimmed.isEmpty()) {
					keywords.add(trimmed);
				}
			}
			routes.add(new Route(name.trim(), engineId.trim(), keywords, weight));
		}

		this.defaultRouteEngineId = trimOrNull(this.smssProp.getProperty(DEFAULT_ROUTE_ENGINE_ID));
		String mode = this.smssProp.getProperty(CLASSIFIER_MODE);
		if (mode != null && !mode.trim().isEmpty()) {
			this.classifierMode = mode.trim().toLowerCase();
		}
		this.classifierEngineId = trimOrNull(this.smssProp.getProperty(CLASSIFIER_ENGINE_ID));
		this.embeddingsEngineId = trimOrNull(this.smssProp.getProperty(EMBEDDINGS_ENGINE_ID));
	}

	private static String trimOrNull(String s) {
		return (s != null && !s.trim().isEmpty()) ? s.trim() : null;
	}

	/** Gson DTO for the router JSON file. Field names must match JSON keys. */
	private static class RouterConfig {
		String mode;
		String default_route;
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
	@SuppressWarnings("deprecation")
	protected AskModelEngineResponse askCall(String question, Object fullPrompt, String context,
			Insight insight, String roomId, Map<String, Object> hyperParameters) {

		String routeEngineId = selectRoute(question, insight);
		classLogger.info("[ModelRouter] '{}' -> question=\"{}\" | routing to engineId={}",
				this.engineId, question, routeEngineId);

		IModelEngine targetEngine = resolveEngine(routeEngineId);
		// Delegate via the public ask() API; inference logs for the target engine
		// are written by that engine's own AbstractModelEngine wrapper.
		return targetEngine.ask(question, context, insight, hyperParameters);
	}

	@Override
	protected EmbeddingsModelEngineResponse embeddingsCall(List<String> stringsToEmbed,
			Insight insight, Map<String, Object> parameters) {

		String engId = this.embeddingsEngineId != null ? this.embeddingsEngineId : fallbackEngineId();
		IModelEngine targetEngine = resolveEngine(engId);
		return targetEngine.embeddings(stringsToEmbed, insight, parameters);
	}

	// -------------------------------------------------------------------------
	// Routing logic
	// -------------------------------------------------------------------------

	private String selectRoute(String question, Insight insight) {
		if (MODE_WEIGHTED.equalsIgnoreCase(this.classifierMode)) {
			return selectRouteByWeight();
		}
		if (MODE_LLM.equalsIgnoreCase(this.classifierMode) && this.classifierEngineId != null) {
			return selectRouteByLLM(question, insight);
		}
		return selectRouteByKeyword(question);
	}

	/**
	 * Weighted round-robin routing: distributes traffic in strict proportion to
	 * ROUTE_x_WEIGHT. A counter cycles 0..total-1 and each route owns a slice.
	 * e.g. weights 30/70 → positions 0-29 = claude, 30-99 = gpt, repeating exactly.
	 * Guarantees no route is starved — the split is exact over every full cycle.
	 */
	private String selectRouteByWeight() {
		int sum = 0;
		for (Route r : routes) {
			if (r.weight > 0) sum += r.weight;
		}
		final int total = sum;
		if (total <= 0) {
			classLogger.warn("[ModelRouter] weighted mode but no positive ROUTE_x_WEIGHT set — using fallback engine");
			return fallbackEngineId();
		}
		// Atomically grab the next position in the cycle and wrap at total
		int pos = rrCounter.getAndUpdate(c -> (c + 1) % total);
		int cumulative = 0;
		for (Route r : routes) {
			if (r.weight <= 0) continue;
			cumulative += r.weight;
			if (pos < cumulative) {
				classLogger.info("[ModelRouter] Round-robin pos {}/{} -> route '{}' (weight {})",
						pos, total, r.name, r.weight);
				return r.engineId;
			}
		}
		return fallbackEngineId();
	}

	/**
	 * Keyword routing: returns the first route whose keyword list contains a match
	 * anywhere in the lower-cased question. Falls back to the default engine.
	 */
	private String selectRouteByKeyword(String question) {
		String lowerQ = question.toLowerCase();
		for (Route route : routes) {
			for (String kw : route.keywords) {
				if (lowerQ.contains(kw)) {
					classLogger.info("[ModelRouter] Keyword '{}' matched route '{}'", kw, route.name);
					return route.engineId;
				}
			}
		}
		classLogger.info("[ModelRouter] No keyword matched \u2014 using fallback engine");
		return fallbackEngineId();
	}

	/**
	 * LLM routing: sends a compact classification prompt to the classifier engine,
	 * expects exactly one route name back, then resolves it. Gracefully degrades to
	 * keyword routing if the LLM call fails or returns an unrecognised name.
	 */
	@SuppressWarnings("deprecation")
	private String selectRouteByLLM(String question, Insight insight) {
		Insight classificationInsight = new Insight();
		InsightStore.getInstance().put(classificationInsight);
		try {
			StringBuilder routeList = new StringBuilder();
			for (Route r : routes) {
				routeList.append("- ").append(r.name);
				if (r.keywords != null && !r.keywords.isEmpty()) {
					routeList.append(" (for questions about: ")
							.append(String.join(", ", r.keywords))
							.append(")");
				}
				routeList.append("\n");
			}

			String classificationPrompt =
					"You are a routing classifier. Given the user question below, "
					+ "reply with ONLY the single route name that best matches — no explanation, no punctuation, no quotes.\n\n"
					+ "Available routes:\n" + routeList
					+ "\nUser question: " + question
					+ "\n\nRoute name:";

			IModelEngine classifierEngine = resolveEngine(this.classifierEngineId);

			// Run classification in an ISOLATED room/insight so it never pollutes the
			// caller's conversation. use_history=false keeps it a clean one-shot call.
			Room room = RoomUtils.createRoomIfNotExists(
					UUID.randomUUID().toString(), classificationInsight, classifierEngine, classificationPrompt);
			Map<String, Object> params = new HashMap<>();
			params.put("use_history", false);
			InputMessage msg = InputMessage.builder(room)
					.withText(classificationPrompt)
					.withModelType(classifierEngine.getModelType())
					.withParamMap(params)
					.build();
			ResponseMessage response = room.ask(msg, classifierEngine);
			Object responseObj = response.getModelEngineResponse().toMap().get("response");
			String routeName = responseObj != null ? responseObj.toString().trim() : "";

			for (Route route : routes) {
				if (route.name.equalsIgnoreCase(routeName)) {
					classLogger.info("[ModelRouter] LLM classified question as route '{}'", routeName);
					return route.engineId;
				}
			}
			classLogger.warn("ModelRouterEngine: LLM returned unknown route '{}', falling back to keyword", routeName);
		} catch (Exception e) {
			classLogger.error("ModelRouterEngine: LLM classification failed, falling back to keyword", e);
		} finally {
			InsightStore.getInstance().remove(classificationInsight.getInsightId());
		}
		return selectRouteByKeyword(question);
	}

	// -------------------------------------------------------------------------
	// Helpers
	// -------------------------------------------------------------------------

	private String fallbackEngineId() {
		if (this.defaultRouteEngineId != null && !this.defaultRouteEngineId.isEmpty()) {
			return this.defaultRouteEngineId;
		}
		if (!routes.isEmpty()) {
			return routes.get(0).engineId;
		}
		throw new IllegalStateException("ModelRouterEngine: no routes configured and no default engine set");
	}

	private IModelEngine resolveEngine(String engineId) {
		IModelEngine engine = (IModelEngine) Utility.getEngine(engineId);
		if (engine == null) {
			throw new IllegalStateException("ModelRouterEngine: could not load engine with id=" + engineId);
		}
		return engine;
	}
}
