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

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.UnaryOperator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.auth.User;
import prerna.auth.utils.SecurityCatalogDependencyUtils.Dependency;
import prerna.engine.api.IEngine;
import prerna.engine.impl.vector.ProxyVectorDatabaseEngine;
import prerna.util.Constants;

/**
 * Works out the automatic dependencies of a catalog item from its own
 * configuration, and saves them. Each save reactor calls the sync for what it
 * saved; the derive methods are pure, given how to read an id's type, so they
 * can be tested without a security database.
 *
 * <p>
 * A sync never fails the save that calls it: the configuration is the source of
 * truth and was saved already, so a dependency that cannot be recorded is
 * logged and left for the backfill.
 * </p>
 */
public final class CatalogDependencyDerivation {

	private static final Logger classLogger = LogManager.getLogger(CatalogDependencyDerivation.class);

	/** The roles read from an engine's smss */
	public static final List<CatalogDependencyRole> SMSS_ROLES = List.of(CatalogDependencyRole.JUDGE_MODEL,
			CatalogDependencyRole.TOPIC_VECTOR, CatalogDependencyRole.EMBEDDER, CatalogDependencyRole.FUNCTION_STORAGE,
			CatalogDependencyRole.KEYWORD_ENGINE, CatalogDependencyRole.DOCUMENT_PROCESSOR,
			CatalogDependencyRole.PROXY_TARGET, CatalogDependencyRole.IMAGE_MODEL);
	/** The roles read from a model router's router.json */
	public static final List<CatalogDependencyRole> ROUTER_ROLES = List.of(CatalogDependencyRole.ROUTE_MODEL,
			CatalogDependencyRole.ROUTER_CLASSIFIER, CatalogDependencyRole.ROUTER_EMBEDDINGS,
			CatalogDependencyRole.ROUTER_FALLBACK);

	/** The smss key of the model a guardrail asks to evaluate the text */
	static final String MODEL_ENGINE_ID = "MODEL_ENGINE_ID";
	/** The smss key of the vector database an On Topic guardrail compares with */
	static final String VECTOR_ENGINE_ID = "VECTOR_ENGINE_ID";
	/** The smss key of the model a vector database embeds with */
	static final String EMBEDDER_ENGINE_ID = Constants.EMBEDDER_ENGINE_ID;
	/**
	 * The legacy smss key of a vector database's embedding model, read only when
	 * {@link #EMBEDDER_ENGINE_ID} is not set, as the vector database reads it
	 */
	static final String ENCODER_ID = "ENCODER_ID";
	/** The smss key of the model a vector database generates keywords with */
	static final String KEYWORD_ENGINE_ID = Constants.KEYWORD_ENGINE_ID;
	/** The smss key that turns on a vector database's custom document processor */
	static final String CUSTOM_DOCUMENT_PROCESSOR = Constants.CUSTOM_DOCUMENT_PROCESSOR;
	/** The smss key of the function a vector database processes its documents with */
	static final String CUSTOM_DOCUMENT_PROCESSOR_FUNCTION_ID = Constants.CUSTOM_DOCUMENT_PROCESSOR_FUNCTION_ID;
	/** The smss key of the vector database a proxy vector database calls */
	static final String TARGET_ENGINE_ID = ProxyVectorDatabaseEngine.TARGET_ENGINE_ID;
	/** The smss key of the storage engine a Google bucket function reads */
	static final String GOOGLE_BUCKET_ENGINEID = "GOOGLE_BUCKET_ENGINEID";
	/** The smss key of the storage engine an AWS Textract or Transcribe function reads */
	static final String S3BUCKETENGINEID = "S3BUCKETENGINEID";
	/** The smss key of the model an image description function describes images with */
	static final String IMAGE_ENGINE_ID = Constants.IMAGE_ENGINE_ID;

	private CatalogDependencyDerivation() {

	}

	/*
	 * Derive
	 */

	/**
	 * The guardrails an engine's pipeline.json runs, each once with every place it
	 * checks.
	 *
	 * @param pipelines the pipelines map: method name to {input, output}, each a list
	 *                  of checks with params.guardrailEngineId
	 * @param typeOf    reads an id's catalog type, or null when it is not known
	 * @return a GUARDRAIL dependency for each guardrail
	 */
	public static List<Dependency> fromGuardrailPipelines(Map<?, ?> pipelines, UnaryOperator<String> typeOf) {
		Map<String, Set<String>> places = new LinkedHashMap<>();
		if (pipelines == null) {
			return List.of();
		}
		for (Map.Entry<?, ?> pipelineEntry : pipelines.entrySet()) {
			if (!(pipelineEntry.getValue() instanceof Map<?, ?> pipeline)) {
				continue;
			}
			String method = methodLabel(String.valueOf(pipelineEntry.getKey()));
			for (String slot : new String[] { "input", "output" }) {
				if (!(pipeline.get(slot) instanceof List<?> checks)) {
					continue;
				}
				for (Object check : checks) {
					if (check instanceof Map<?, ?> checkMap && checkMap.get("params") instanceof Map<?, ?> params) {
						String id = trimmed(params.get("guardrailEngineId"));
						if (id != null) {
							places.computeIfAbsent(id, key -> new LinkedHashSet<>()).add(method + " " + slot);
						}
					}
				}
			}
		}
		List<Dependency> dependencies = new ArrayList<>();
		for (Map.Entry<String, Set<String>> entry : places.entrySet()) {
			add(dependencies, typeOf, entry.getKey(), CatalogDependencyRole.GUARDRAIL,
					"Guardrail that checks this engine's " + String.join(", ", entry.getValue()));
		}
		return dependencies;
	}

	/**
	 * The engines an engine's smss names: a guardrail's judge model or vector
	 * database; a vector database's embedding model, keyword model, document
	 * processor function, or a proxy's target vector database; and a function's
	 * Google or S3 bucket storage engine or image description model.
	 *
	 * @param engineType the engine's type, such as GUARDRAIL
	 * @param smss       its smss properties or creation details
	 * @param typeOf     reads an id's catalog type, or null when it is not known
	 * @return the dependencies, under {@link #SMSS_ROLES}
	 */
	public static List<Dependency> fromSmss(String engineType, Map<?, ?> smss, UnaryOperator<String> typeOf) {
		List<Dependency> dependencies = new ArrayList<>();
		if (smss == null || engineType == null) {
			return dependencies;
		}
		String type = engineType.trim().toUpperCase(java.util.Locale.ROOT);
		if (IEngine.CATALOG_TYPE.GUARDRAIL.name().equals(type)) {
			add(dependencies, typeOf, trimmed(smss.get(MODEL_ENGINE_ID)), CatalogDependencyRole.JUDGE_MODEL,
					"Model that evaluates text for this guardrail");
			add(dependencies, typeOf, trimmed(smss.get(VECTOR_ENGINE_ID)), CatalogDependencyRole.TOPIC_VECTOR,
					"Vector database this guardrail compares text with to stay on topic");
		} else if (IEngine.CATALOG_TYPE.VECTOR.name().equals(type)) {
			// the vector database falls back to the legacy key only when the current one
			// is not set
			String embedderId = trimmed(smss.get(EMBEDDER_ENGINE_ID));
			if (embedderId == null) {
				embedderId = trimmed(smss.get(ENCODER_ID));
			}
			add(dependencies, typeOf, embedderId, CatalogDependencyRole.EMBEDDER,
					"Model that creates this vector database's embeddings");
			add(dependencies, typeOf, trimmed(smss.get(KEYWORD_ENGINE_ID)), CatalogDependencyRole.KEYWORD_ENGINE,
					"Model that generates keywords for this vector database's documents");
			// the processor function is called only when the custom processor is on
			if (Boolean.parseBoolean(trimmed(smss.get(CUSTOM_DOCUMENT_PROCESSOR)))) {
				add(dependencies, typeOf, trimmed(smss.get(CUSTOM_DOCUMENT_PROCESSOR_FUNCTION_ID)),
						CatalogDependencyRole.DOCUMENT_PROCESSOR,
						"Function that processes this vector database's documents");
			}
			add(dependencies, typeOf, trimmed(smss.get(TARGET_ENGINE_ID)), CatalogDependencyRole.PROXY_TARGET,
					"Vector database this proxy sends its calls to");
		} else if (IEngine.CATALOG_TYPE.FUNCTION.name().equals(type)) {
			add(dependencies, typeOf, trimmed(smss.get(GOOGLE_BUCKET_ENGINEID)), CatalogDependencyRole.FUNCTION_STORAGE,
					"Storage engine this function reads its bucket from");
			add(dependencies, typeOf, trimmed(smss.get(S3BUCKETENGINEID)), CatalogDependencyRole.FUNCTION_STORAGE,
					"Storage engine this function reads its bucket from");
			add(dependencies, typeOf, trimmed(smss.get(IMAGE_ENGINE_ID)), CatalogDependencyRole.IMAGE_MODEL,
					"Model that describes the images this function finds in documents");
		}
		return dependencies;
	}

	/**
	 * The models a model router's router.json sends to.
	 *
	 * @param config the router.json map: routes[].engine_id, classifier_engine,
	 *               embeddings_engine and fallbacks
	 * @param typeOf reads an id's catalog type, or null when it is not known
	 * @return the dependencies, under {@link #ROUTER_ROLES}
	 */
	public static List<Dependency> fromRouterConfig(Map<?, ?> config, UnaryOperator<String> typeOf) {
		List<Dependency> dependencies = new ArrayList<>();
		if (config == null) {
			return dependencies;
		}
		Map<String, Set<String>> routesByModel = new LinkedHashMap<>();
		if (config.get("routes") instanceof List<?> routes) {
			for (Object route : routes) {
				if (route instanceof Map<?, ?> routeMap) {
					String id = trimmed(routeMap.get("engine_id"));
					if (id != null) {
						String name = trimmed(routeMap.get("name"));
						routesByModel.computeIfAbsent(id, key -> new LinkedHashSet<>())
								.add(name == null ? "unnamed" : "'" + name + "'");
					}
				}
			}
		}
		for (Map.Entry<String, Set<String>> entry : routesByModel.entrySet()) {
			add(dependencies, typeOf, entry.getKey(), CatalogDependencyRole.ROUTE_MODEL,
					"Model this router sends the " + String.join(", ", entry.getValue())
							+ (entry.getValue().size() == 1 ? " route" : " routes") + " to");
		}
		add(dependencies, typeOf, trimmed(config.get("classifier_engine")), CatalogDependencyRole.ROUTER_CLASSIFIER,
				"Model this router classifies requests with");
		add(dependencies, typeOf, trimmed(config.get("embeddings_engine")), CatalogDependencyRole.ROUTER_EMBEDDINGS,
				"Model this router embeds requests with");
		if (config.get("fallbacks") instanceof List<?> fallbacks) {
			for (Object fallback : fallbacks) {
				add(dependencies, typeOf, trimmed(fallback), CatalogDependencyRole.ROUTER_FALLBACK,
						"Model this router falls back to when a route fails");
			}
		}
		return dependencies;
	}

	/**
	 * The models a model router's router.json sends to, from its JSON
	 *
	 * @param json   the router.json text, or null
	 * @param typeOf reads an id's catalog type, or null when it is not known
	 * @return the dependencies, or none when the JSON cannot be read
	 */
	public static List<Dependency> fromRouterConfigJson(String json, UnaryOperator<String> typeOf) {
		if (json == null || json.isBlank()) {
			return List.of();
		}
		try {
			return fromRouterConfig(new Gson().fromJson(json, Map.class), typeOf);
		} catch (RuntimeException e) {
			classLogger.warn("Unable to read the router configuration for its dependencies", e);
			return List.of();
		}
	}

	/** The node config keys that hold a catalog id */
	private static final String[] AUTOMATION_ID_KEYS = { "engineId", "appId", "workspaceId" };

	/**
	 * The engines, apps and agents an automation's nodes use, each once with the
	 * nodes that use it.
	 *
	 * @param nodes  the definition's nodes, each with label and config
	 * @param typeOf reads an id's catalog type, or null when it is not known
	 * @return an AUTOMATION_NODE dependency for each
	 */
	public static List<Dependency> fromAutomationNodes(Collection<Map<String, Object>> nodes,
			UnaryOperator<String> typeOf) {
		Map<String, Set<String>> nodesById = new LinkedHashMap<>();
		if (nodes == null) {
			return List.of();
		}
		for (Map<String, Object> node : nodes) {
			if (!(node.get("config") instanceof Map<?, ?> config)) {
				continue;
			}
			String label = trimmed(node.get("label"));
			if (label == null) {
				label = trimmed(node.get("id"));
			}
			for (String key : AUTOMATION_ID_KEYS) {
				String id = trimmed(config.get(key));
				if (id != null) {
					nodesById.computeIfAbsent(id, k -> new LinkedHashSet<>()).add(label == null ? "a node" : "'" + label + "'");
				}
			}
		}
		List<Dependency> dependencies = new ArrayList<>();
		for (Map.Entry<String, Set<String>> entry : nodesById.entrySet()) {
			add(dependencies, typeOf, entry.getKey(), CatalogDependencyRole.AUTOMATION_NODE,
					"Used by the automation " + (entry.getValue().size() == 1 ? "step " : "steps ")
							+ String.join(", ", entry.getValue()));
		}
		return dependencies;
	}

	/**
	 * The automatic dependencies of an agent from what it has saved: its
	 * resources (tools, knowledge and skills; prompts are not catalog items), and
	 * the model and subagents in its CONFIG_JSON. For the backfill; a save reads
	 * the same from the request.
	 *
	 * @param resources   its WORKSPACE_RESOURCE rows, each with resource_id and
	 *                    resource_type
	 * @param modelId     CONFIG_JSON.model_id, or null
	 * @param subagentIds the workspaceId of each CONFIG_JSON.subagents entry
	 * @param typeOf      reads an id's catalog type, or null when it is not known
	 * @return the dependencies, under TOOLBOX, KNOWLEDGE, SKILL, AGENT_MODEL and
	 *         SUBAGENT
	 */
	public static List<Dependency> fromWorkspace(Collection<Map<String, Object>> resources, String modelId,
			Collection<String> subagentIds, UnaryOperator<String> typeOf) {
		List<Dependency> dependencies = new ArrayList<>();
		if (resources != null) {
			for (Map<String, Object> resource : resources) {
				String id = trimmed(resource.get("resource_id"));
				String resourceType = trimmed(resource.get("resource_type"));
				if (id == null || "PROMPT".equalsIgnoreCase(resourceType)) {
					continue;
				}
				String type = typeOf.apply(id);
				if ("SKILL".equalsIgnoreCase(resourceType)) {
					add(dependencies, typeOf, id, CatalogDependencyRole.SKILL, "Skill attached to this agent");
				} else if (IEngine.CATALOG_TYPE.VECTOR.name().equals(type)) {
					add(dependencies, typeOf, id, CatalogDependencyRole.KNOWLEDGE, "Knowledge base this agent searches");
				} else {
					add(dependencies, typeOf, id, CatalogDependencyRole.TOOLBOX, "Tool this agent can call");
				}
			}
		}
		add(dependencies, typeOf, trimmed(modelId), CatalogDependencyRole.AGENT_MODEL, "Model this agent runs on");
		if (subagentIds != null) {
			for (String subagentId : subagentIds) {
				add(dependencies, typeOf, trimmed(subagentId), CatalogDependencyRole.SUBAGENT,
						"Subagent this agent can delegate to");
			}
		}
		return dependencies;
	}

	/** The roles {@link #fromWorkspace} derives */
	public static final List<CatalogDependencyRole> WORKSPACE_ROLES = List.of(CatalogDependencyRole.TOOLBOX,
			CatalogDependencyRole.KNOWLEDGE, CatalogDependencyRole.SKILL, CatalogDependencyRole.AGENT_MODEL,
			CatalogDependencyRole.SUBAGENT);

	/*
	 * Sync
	 */

	/**
	 * Saves the guardrails an engine's pipeline.json runs as its GUARDRAIL
	 * dependencies.
	 *
	 * @param user       who saved the configuration
	 * @param engineId   the guarded engine
	 * @param engineType its type
	 * @param pipelines  the saved pipelines map
	 */
	public static void syncGuardrails(User user, String engineId, String engineType, Map<?, ?> pipelines) {
		sync(user, engineId, engineType, List.of(CatalogDependencyRole.GUARDRAIL),
				() -> fromGuardrailPipelines(pipelines, CatalogTypes::resolve));
	}

	/**
	 * Saves the engines an engine's smss names as its dependencies.
	 *
	 * @param user       who created or changed the engine
	 * @param engineId   the engine
	 * @param engineType its type
	 * @param smss       its smss properties or creation details
	 */
	public static void syncSmss(User user, String engineId, String engineType, Map<?, ?> smss) {
		sync(user, engineId, engineType, SMSS_ROLES, () -> fromSmss(engineType, smss, CatalogTypes::resolve));
	}

	/**
	 * Saves the models a model router sends to as its dependencies.
	 *
	 * @param user     who saved the router
	 * @param engineId the router
	 * @param config   its router.json map
	 */
	public static void syncRouter(User user, String engineId, Map<?, ?> config) {
		sync(user, engineId, IEngine.CATALOG_TYPE.MODEL.name(), ROUTER_ROLES,
				() -> fromRouterConfig(config, CatalogTypes::resolve));
	}

	/**
	 * Saves the models a model router sends to as its dependencies, from its JSON.
	 *
	 * @param user     who created the router
	 * @param engineId the router
	 * @param json     its router.json text
	 */
	public static void syncRouterJson(User user, String engineId, String json) {
		sync(user, engineId, IEngine.CATALOG_TYPE.MODEL.name(), ROUTER_ROLES,
				() -> fromRouterConfigJson(json, CatalogTypes::resolve));
	}

	/**
	 * Saves the engines, apps and agents an automation's nodes use as its
	 * dependencies.
	 *
	 * @param user      who saved the automation
	 * @param projectId the automation
	 * @param nodes     the saved definition's nodes
	 */
	public static void syncAutomation(User user, String projectId, Collection<Map<String, Object>> nodes) {
		sync(user, projectId, prerna.project.api.IProject.PROJECT_TYPE.AUTOMATION.name(),
				List.of(CatalogDependencyRole.AUTOMATION_NODE), () -> fromAutomationNodes(nodes, CatalogTypes::resolve));
	}

	private static void sync(User user, String catalogId, String catalogType, List<CatalogDependencyRole> roles,
			java.util.function.Supplier<List<Dependency>> derive) {
		try {
			SecurityCatalogDependencyUtils.syncAutomaticDependencies(user, catalogId, catalogType, roles,
					derive.get());
		} catch (RuntimeException e) {
			classLogger.warn("Saved {} but could not record its {} dependencies", catalogId, roles, e);
		}
	}

	/*
	 * Helpers
	 */

	private static void add(List<Dependency> dependencies, UnaryOperator<String> typeOf, String id,
			CatalogDependencyRole role, String description) {
		if (id == null) {
			return;
		}
		String type = typeOf.apply(id);
		if (type == null) {
			classLogger.warn("{} is not an engine or a project, so it is not recorded as a {} dependency", id, role);
			return;
		}
		dependencies.add(Dependency.automatic(id, type, role, description));
	}

	private static String methodLabel(String method) {
		if (method.isBlank() || "*".equals(method) || "ALL".equalsIgnoreCase(method)) {
			return "every method's";
		}
		return method;
	}

	private static String trimmed(Object value) {
		if (!(value instanceof String text)) {
			return null;
		}
		String trimmed = text.trim();
		return trimmed.isEmpty() ? null : trimmed;
	}
}
