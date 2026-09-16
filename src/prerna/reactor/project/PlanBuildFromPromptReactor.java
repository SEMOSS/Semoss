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
package prerna.reactor.project;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.google.gson.reflect.TypeToken;

import prerna.auth.AccessToken;
import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.auth.utils.SecurityProjectUtils;
import prerna.auth.utils.SecurityUserUtils;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.Room;
import prerna.engine.impl.model.RoomUtils;
import prerna.engine.impl.model.message.InputMessage;
import prerna.engine.impl.model.message.ResponseMessage;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Turns one natural-language prompt into a plan for what to create: the target
 * kind and type, the closest existing template, the best agent to build it, and
 * a suggested name.
 *
 * Side-effect free by design. It creates nothing and reserves no name - the
 * caller executes the plan (CreateAppFromTemplate, CloneSkill, CreateProject or
 * CreateEmptyRdbmsDatabase) and is responsible for name de-duplication.
 *
 * The work is deliberately split into two model calls. The candidate template
 * set is a function of the classified type, so asking for both at once would
 * mean putting templates of every type into the prompt - several times the
 * tokens, a wider permission surface, and a worse pick because the model would
 * be doing type reasoning and template matching in one shot. The classify call
 * is cheap and is skipped entirely when the caller already knows the type and
 * passes it in.
 */
public class PlanBuildFromPromptReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(PlanBuildFromPromptReactor.class);

	/** Project types that can host the workbench assistant. */
	private static final Set<String> PROJECT_TARGETS = new LinkedHashSet<>(
			Arrays.asList("CODE", "NOTEBOOK", "SKILL", "WORKSPACE"));

	/**
	 * Engine types, mirroring IEngine.CATALOG_TYPE minus the internal ones.
	 * Only DATABASE can actually be created from a prompt alone; the rest need
	 * connection details, and the caller routes those to their import form.
	 */
	private static final Set<String> ENGINE_TARGETS = new LinkedHashSet<>(
			Arrays.asList("DATABASE", "VECTOR", "STORAGE", "FUNCTION", "GUARDRAIL", "MODEL"));

	/**
	 * BLOCKS and INSIGHTS are deliberately absent from PROJECT_TARGETS: they
	 * still render on the legacy workspace shell, which has no assistant, so a
	 * prompt handed to one would be silently dropped.
	 */
	private static final String DEFAULT_PROJECT_TARGET = "CODE";

	private static final int MAX_TEMPLATE_FETCH = 50;
	private static final int MAX_TEMPLATE_PROMPTED = 12;
	private static final int MAX_AGENT_FETCH = 30;
	private static final int MAX_AGENT_PROMPTED = 8;
	private static final int MAX_DESC_CHARS = 240;
	private static final int MAX_PROMPT_CHARS = 1000;
	private static final int MAX_NAME_CHARS = 60;

	private static final Set<String> STOPWORDS = new HashSet<>(Arrays.asList("the", "and", "for", "with", "that",
			"this", "from", "into", "you", "your", "can", "will", "would", "should", "build", "create", "make", "need",
			"want", "help", "app", "application", "new", "some", "using", "use", "about", "all", "any", "are", "how"));

	public PlanBuildFromPromptReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.PROMPT.getKey(), ReactorKeysEnum.PROJECT_TYPE.getKey(),
				ReactorKeysEnum.ENGINE.getKey(), ReactorKeysEnum.LIMIT.getKey() };
		this.keyRequired = new int[] { 1, 0, 0, 0 };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();

		Map<String, Object> result = new LinkedHashMap<>();
		// Machine-readable trail of every step that fell back, so the caller can
		// explain itself to the user without us throwing.
		List<String> degraded = new ArrayList<>();

		try {
			User user = this.insight.getUser();
			if (user == null) {
				throw new IllegalArgumentException("You are not properly logged in");
			}

			String prompt = truncate(clean(this.keyValue.get(ReactorKeysEnum.PROMPT.getKey())), MAX_PROMPT_CHARS);
			if (prompt.isBlank()) {
				throw new IllegalArgumentException("prompt cannot be empty");
			}

			String engineId = resolveEngine(user, clean(this.keyValue.get(ReactorKeysEnum.ENGINE.getKey())), degraded);
			if (!SecurityEngineUtils.userCanViewEngine(user, engineId)) {
				throw new IllegalArgumentException(
						"Model " + engineId + " does not exist or user does not have access to this model");
			}
			IModelEngine model = Utility.getModel(engineId);

			// ---- step 1: what kind of thing does this prompt want ----
			String typeOverride = normalizeTarget(clean(this.keyValue.get(ReactorKeysEnum.PROJECT_TYPE.getKey())));
			String targetType;
			String targetTypeSource;
			if (!typeOverride.isEmpty()) {
				targetType = typeOverride;
				targetTypeSource = "client";
			} else {
				Map<String, Object> classified = askJson(model, buildClassifyPrompt(prompt), degraded, "classify");
				targetType = normalizeTarget(readString(classified, "targetType"));
				if (targetType.isEmpty()) {
					targetType = DEFAULT_PROJECT_TARGET;
					targetTypeSource = "default";
					degraded.add("target_type_defaulted");
				} else {
					targetTypeSource = "model";
				}
			}
			boolean isProject = PROJECT_TARGETS.contains(targetType);

			// ---- step 2: candidates ----
			// Templates are projects only (there is no such thing as an engine
			// template), so the engine path skips the template fetch entirely.
			List<Map<String, Object>> rankedTemplates = Collections.emptyList();
			if (isProject) {
				int limit = clampLimit(this.keyValue.get(ReactorKeysEnum.LIMIT.getKey()));
				// Every candidate that reaches the model comes from
				// getUserProjectList, which filters by visibility inside the
				// query. Do not add candidates from any other source here or a
				// template the user cannot see could be named back to us.
				List<Map<String, Object>> templates = fetchTemplates(user, targetType, limit);
				rankedTemplates = rankByOverlap(templates, prompt, MAX_TEMPLATE_PROMPTED);
			}

			List<Map<String, Object>> agents = SecurityProjectUtils.getUserProjectList(user,
					Arrays.asList("WORKSPACE"), null, false, null, null, null, String.valueOf(MAX_AGENT_FETCH), "0",
					null, false);
			List<Map<String, Object>> rankedAgents = rankByOverlap(agents, prompt, MAX_AGENT_PROMPTED);

			// ---- step 3: one call for template + agent + name ----
			Map<String, Object> pick = Collections.emptyMap();
			if (!rankedTemplates.isEmpty() || !rankedAgents.isEmpty()) {
				pick = askJson(model, buildPickPrompt(prompt, targetType, isProject, rankedTemplates, rankedAgents),
						degraded, "pick");
			} else {
				degraded.add("no_candidates_available");
			}

			// ---- step 4: validate every field on its own, degrading as needed ----
			String templateId = null;
			String templateName = null;
			String templateType = null;
			String confidence = normalizeConfidence(readString(pick, "templateConfidence"));
			if (isProject) {
				String proposed = readString(pick, "templateProjectId");
				Map<String, Object> matched = findById(rankedTemplates, proposed);
				if (!proposed.isEmpty() && matched == null) {
					// Check membership of the list we actually offered rather
					// than asking the database whether the id exists - a real
					// but un-offered project would pass an existence check.
					degraded.add("template_id_not_in_candidates");
				} else if (matched != null && !"HIGH".equals(confidence) && !"MEDIUM".equals(confidence)) {
					degraded.add("template_confidence_low");
				} else if (matched != null
						&& !SecurityProjectUtils.userCanCloneProject(user, stringValue(matched.get("project_id")))) {
					// Catches a template whose IS_TEMPLATE flag changed between
					// the fetch and the pick.
					degraded.add("template_not_clonable");
				} else if (matched != null) {
					templateId = stringValue(matched.get("project_id"));
					templateName = displayName(matched);
					templateType = stringValue(matched.get("project_type"));
				}
			}
			if (templateId == null && "NONE".equals(confidence)) {
				confidence = "NONE";
			} else if (templateId == null) {
				confidence = rankedTemplates.isEmpty() && isProject ? "NONE" : confidence;
			}

			String agentId = Constants.AGENT_APP_BUILDER;
			String agentName = null;
			boolean agentIsDefault = true;
			String proposedAgent = readString(pick, "agentProjectId");
			Map<String, Object> matchedAgent = findById(rankedAgents, proposedAgent);
			if (!proposedAgent.isEmpty() && matchedAgent == null) {
				degraded.add("agent_id_not_in_candidates");
			} else if (matchedAgent != null) {
				String candidateId = stringValue(matchedAgent.get("project_id"));
				if (SecurityProjectUtils.userCanViewProject(user, candidateId)) {
					agentId = candidateId;
					agentName = displayName(matchedAgent);
					agentIsDefault = Constants.AGENT_APP_BUILDER.equals(candidateId);
				} else {
					degraded.add("agent_not_viewable");
				}
			}

			String name = sanitizeName(readString(pick, "projectName"));
			if (name.isEmpty()) {
				name = deriveNameFromPrompt(prompt);
				degraded.add("project_name_derived_locally");
			}

			result.put("success", true);
			result.put("targetKind", isProject ? "PROJECT" : "ENGINE");
			result.put("targetType", targetType);
			result.put("targetTypeSource", targetTypeSource);
			result.put("projectName", name);
			result.put("templateProjectId", templateId);
			result.put("templateProjectName", templateName);
			result.put("templateProjectType", templateType);
			result.put("templateMatchConfidence", templateId == null ? "NONE" : confidence);
			result.put("useBlankApp", templateId == null);
			result.put("agentProjectId", agentId);
			result.put("agentProjectName", agentName);
			result.put("agentIsDefault", agentIsDefault);
			result.put("engineId", engineId);
			result.put("templateCandidateCount", rankedTemplates.size());
			result.put("agentCandidateCount", rankedAgents.size());
			result.put("degraded", degraded);
			return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
		} catch (Exception e) {
			classLogger.warn("PlanBuildFromPrompt failed: {}", e.getMessage());
			result.clear();
			result.put("success", false);
			result.put("error", e.getMessage() == null ? "Could not plan anything from that prompt" : e.getMessage());
			return new NounMetadata(result, PixelDataType.MAP, PixelOperationType.OPERATION);
		}
	}

	/**
	 * Resolve which model reasons about the prompt: the one passed in, else the
	 * user's configured default text-generation model, else the first one they
	 * can see. Mirrors the client's getDefaultWorkbenchAssistantModel so the
	 * caller does not have to make two extra pixel calls before this one.
	 */
	private String resolveEngine(User user, String requested, List<String> degraded) {
		if (!requested.isBlank()) {
			return requested;
		}

		AccessToken token = user.getPrimaryLoginToken();
		if (token != null) {
			Map<String, Collection<String>> meta = SecurityUserUtils.getAggregateUserMetadata(token.getId(),
					token.getProvider(), Arrays.asList(Constants.DEFAULT_TEXT_GENERATION_MODEL_KEY), true);
			Collection<String> configured = meta.get(Constants.DEFAULT_TEXT_GENERATION_MODEL_KEY);
			if (configured != null) {
				for (String value : configured) {
					if (value != null && !value.isBlank() && SecurityEngineUtils.userCanViewEngine(user, value.trim())) {
						degraded.add("engine_from_user_default");
						return value.trim();
					}
				}
			}
		}

		Map<String, Object> textGeneration = new LinkedHashMap<>();
		textGeneration.put("tag", "text-generation");
		List<Map<String, Object>> models = SecurityEngineUtils.getUserEngineList(user, Arrays.asList("MODEL"), null,
				false, textGeneration, null, null, "1", "0");
		if (models != null && !models.isEmpty()) {
			degraded.add("engine_from_first_available");
			return stringValue(models.get(0).get("engine_id"));
		}

		throw new IllegalArgumentException("No text-generation model is available to plan from this prompt");
	}

	/**
	 * Templates of one project type that the user is allowed to see and clone.
	 *
	 * searchTerm stays null on purpose: getUserProjectList applies it as a
	 * regex against the id/name columns, so passing a natural-language sentence
	 * there matches nothing at all. Ranking against the prompt happens locally
	 * in {@link #rankByOverlap}.
	 */
	private List<Map<String, Object>> fetchTemplates(User user, String projectType, int limit) {
		return SecurityProjectUtils.getUserProjectList(user, Arrays.asList(projectType), null, false, null, null, null,
				String.valueOf(limit), "0", null, true);
	}

	/**
	 * Narrow the candidate list by lexical overlap with the prompt before it
	 * reaches the model. This is what keeps the prompt a bounded size no matter
	 * how many templates the instance has. Candidates that score zero are still
	 * kept while there is room, so a badly described but correct template can
	 * still be picked.
	 */
	private List<Map<String, Object>> rankByOverlap(List<Map<String, Object>> candidates, String prompt, int keep) {
		if (candidates == null || candidates.isEmpty()) {
			return Collections.emptyList();
		}

		Set<String> terms = tokenize(prompt);
		List<Map<String, Object>> sorted = new ArrayList<>(candidates);
		sorted.sort((left, right) -> {
			int byScore = Integer.compare(score(right, terms), score(left, terms));
			if (byScore != 0) {
				return byScore;
			}
			// Stable tie-break so the same prompt always produces the same list.
			return displayName(left).compareToIgnoreCase(displayName(right));
		});
		return sorted.subList(0, Math.min(keep, sorted.size()));
	}

	private int score(Map<String, Object> candidate, Set<String> terms) {
		return 2 * overlap(displayName(candidate), terms) + 2 * overlap(stringValue(candidate.get("project_name")), terms)
				+ overlap(description(candidate), terms) + overlap(stringValue(candidate.get("tag")), terms);
	}

	private int overlap(String text, Set<String> terms) {
		int matches = 0;
		for (String token : tokenize(text)) {
			if (terms.contains(token)) {
				matches++;
			}
		}
		return matches;
	}

	private Set<String> tokenize(String text) {
		Set<String> tokens = new LinkedHashSet<>();
		if (text == null) {
			return tokens;
		}
		for (String raw : text.toLowerCase(Locale.ROOT).split("[^a-z0-9]+")) {
			if (raw.length() >= 3 && !STOPWORDS.contains(raw)) {
				tokens.add(raw);
			}
		}
		return tokens;
	}

	static String buildClassifyPrompt(String prompt) {
		return "Classify what the user wants to create.\n\n"
				+ "Allowed values for targetType, and nothing else:\n"
				+ "- CODE (PROJECT): a custom application, portal or API written as code.\n"
				+ "- NOTEBOOK (PROJECT): exploratory data analysis, data preparation or modelling. This is a ipynb or jupyter notebook.\n"
				+ "- SKILL (PROJECT): a reusable instruction package an assistant loads to perform a task.\n"
				+ "- WORKSPACE (PROJECT): a configured assistant or agent, with its own prompt and tools.\n"
				+ "- DATABASE (ENGINE): somewhere to store structured data; a schema or set of tables.\n"
				+ "- VECTOR (ENGINE): a vector store for document search or retrieval.\n"
				+ "- STORAGE (ENGINE): a connection to file or object storage.\n"
				+ "- FUNCTION (ENGINE): a callable remote function or service.\n"
				+ "- GUARDRAIL (ENGINE): a content or policy check applied to model traffic.\n"
				+ "- MODEL (ENGINE): a connection to a hosted language or embedding model.\n\n"
				+ "Pick exactly one. When the user describes something they want to look at or interact with, "
				+ "prefer a PROJECT. Choose DATABASE only when the thing being asked for is the data store itself.\n"
				+ "Return ONLY JSON: {\"targetKind\":\"PROJECT\",\"targetType\":\"CODE\"}\n\n"
				+ "USER REQUEST:\n" + prompt + "\n\nJSON:";
	}

	static String buildPickPrompt(String prompt, String targetType, boolean isProject,
			List<Map<String, Object>> templates, List<Map<String, Object>> agents) {
		StringBuilder text = new StringBuilder();
		text.append("The user wants to create a new ").append(targetType)
				.append(isProject ? " project." : " engine.").append("\n");
		text.append("Choose the best assistant to build it and a short name for it");
		if (isProject) {
			text.append(", plus the closest starting template");
		}
		text.append(".\n\n");

		if (isProject) {
			text.append("CANDIDATE TEMPLATES (id | name | type | description):\n");
			if (templates.isEmpty()) {
				text.append("(none available)\n");
			} else {
				int index = 1;
				for (Map<String, Object> template : templates) {
					text.append(index++).append(". ").append(stringValue(template.get("project_id"))).append(" | ")
							.append(displayName(template)).append(" | ")
							.append(stringValue(template.get("project_type"))).append(" | ")
							.append(oneLine(description(template))).append("\n");
				}
			}
			text.append("If none of these is a genuinely close starting point, return \"\" for templateProjectId. ")
					.append("Do not invent an id. Do not pick a template only because it is the same type.\n\n");
		}

		text.append("CANDIDATE ASSISTANTS (id | name | description):\n");
		if (agents.isEmpty()) {
			text.append("(none available)\n");
		} else {
			int index = 1;
			for (Map<String, Object> agent : agents) {
				text.append(index++).append(". ").append(stringValue(agent.get("project_id"))).append(" | ")
						.append(displayName(agent)).append(" | ").append(oneLine(description(agent))).append("\n");
			}
		}
		text.append("Return \"\" for agentProjectId when none is clearly better than a general-purpose builder.\n\n");

		text.append("Return ONLY JSON in exactly this form:\n");
		text.append("{\"templateProjectId\":\"\",\"templateConfidence\":\"HIGH|MEDIUM|LOW|NONE\",")
				.append("\"agentProjectId\":\"\",\"projectName\":\"<3-5 word title, Title Case, no quotes>\"}\n\n");
		text.append("HIGH means the template is built for this exact use case. MEDIUM means it covers most of it ")
				.append("and beats an empty start. LOW or NONE means an empty start is just as good - set ")
				.append("templateProjectId to \"\".\n\n");
		text.append("USER REQUEST:\n").append(prompt).append("\n\nJSON:");
		return text.toString();
	}

	/**
	 * One stateless model call returning a JSON object. Never throws: a failure
	 * here is recorded and the caller carries on with its defaults, because a
	 * model that cannot answer should still leave the user with something
	 * created rather than an error.
	 */
	private Map<String, Object> askJson(IModelEngine model, String text, List<String> degraded, String step) {
		try {
			Room room = RoomUtils.createRoomForStatelessAsk(UUID.randomUUID().toString(), this.insight, model, null);
			ResponseMessage response = room.ask(InputMessage.builder(room).withText(text).build(), model);
			Map<String, Object> parsed = parseJsonObject(responseText(response));
			return parsed == null ? Collections.<String, Object>emptyMap() : parsed;
		} catch (Exception e) {
			classLogger.warn("PlanBuildFromPrompt {} step failed: {}", step, e.getMessage());
			degraded.add(step + "_unparseable");
			return Collections.emptyMap();
		}
	}

	static Map<String, Object> parseJsonObject(String modelOutput) {
		String output = modelOutput == null ? "" : modelOutput.trim();
		int start = output.indexOf('{');
		int end = output.lastIndexOf('}');
		if (start < 0 || end <= start) {
			throw new IllegalArgumentException("Model did not return a JSON object");
		}
		return GSON.fromJson(output.substring(start, end + 1), new TypeToken<Map<String, Object>>() {
		}.getType());
	}

	private static String responseText(ResponseMessage response) {
		String content = response.getContent();
		if (content != null && !content.isBlank()) {
			return content.trim();
		}
		String thinking = response.getThinking();
		return thinking == null ? "" : thinking.trim();
	}

	private static Map<String, Object> findById(List<Map<String, Object>> candidates, String projectId) {
		if (projectId == null || projectId.isBlank()) {
			return null;
		}
		for (Map<String, Object> candidate : candidates) {
			if (projectId.equals(stringValue(candidate.get("project_id")))) {
				return candidate;
			}
		}
		return null;
	}

	/** Uppercase and allow-list, so unrecognised model output never routes anywhere. */
	static String normalizeTarget(String value) {
		String upper = value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
		if (PROJECT_TARGETS.contains(upper) || ENGINE_TARGETS.contains(upper)) {
			return upper;
		}
		return "";
	}

	static String normalizeConfidence(String value) {
		String upper = value == null ? "" : value.trim().toUpperCase(Locale.ROOT);
		if ("HIGH".equals(upper) || "MEDIUM".equals(upper) || "NONE".equals(upper)) {
			return upper;
		}
		return "LOW";
	}

	static String sanitizeName(String value) {
		if (value == null) {
			return "";
		}
		String stripped = value.replaceAll("[^A-Za-z0-9 _-]", " ").replaceAll("\\s+", " ").trim();
		return truncate(stripped, MAX_NAME_CHARS);
	}

	private String deriveNameFromPrompt(String prompt) {
		StringBuilder name = new StringBuilder();
		int words = 0;
		for (String token : tokenize(prompt)) {
			if (words == 4) {
				break;
			}
			name.append(name.length() == 0 ? "" : " ").append(Character.toUpperCase(token.charAt(0)))
					.append(token.substring(1));
			words++;
		}
		String derived = sanitizeName(name.toString());
		return derived.isEmpty() ? "New Project" : derived;
	}

	private int clampLimit(String value) {
		try {
			return Math.min(Math.max(1, Integer.parseInt(clean(value))), MAX_TEMPLATE_FETCH);
		} catch (Exception e) {
			return MAX_TEMPLATE_FETCH;
		}
	}

	private static String displayName(Map<String, Object> project) {
		String display = stringValue(project.get("project_display_name"));
		return display.isEmpty() ? stringValue(project.get("project_name")) : display;
	}

	private static String description(Map<String, Object> project) {
		String description = stringValue(project.get("description"));
		if (description.isEmpty()) {
			description = stringValue(project.get("project_description"));
		}
		return truncate(description, MAX_DESC_CHARS);
	}

	private static String oneLine(String value) {
		return value == null ? "" : value.replaceAll("\\s+", " ").trim();
	}

	private static String readString(Map<String, Object> map, String key) {
		return map == null ? "" : stringValue(map.get(key));
	}

	private static String stringValue(Object value) {
		return value == null ? "" : String.valueOf(value).trim();
	}

	private static String truncate(String value, int max) {
		if (value == null) {
			return "";
		}
		return value.length() > max ? value.substring(0, max) : value;
	}

	private static String clean(Object value) {
		if (value == null) {
			return "";
		}
		String text = String.valueOf(value).trim();
		if (text.length() >= 2
				&& ((text.startsWith("\"") && text.endsWith("\"")) || (text.startsWith("'") && text.endsWith("'")))) {
			return text.substring(1, text.length() - 1).trim();
		}
		return text;
	}

	@Override
	public String getReactorDescription() {
		return "Plans what to create from a natural-language prompt: the target kind and type, the closest existing "
				+ "template, the best agent to build it, and a suggested name. Creates nothing.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.PROMPT.getKey())) {
			return "Natural-language description of what the user wants to build. Truncated to " + MAX_PROMPT_CHARS
					+ " characters.";
		} else if (key.equals(ReactorKeysEnum.PROJECT_TYPE.getKey())) {
			return "Optional target type override (CODE, NOTEBOOK, SKILL, WORKSPACE, DATABASE, VECTOR, STORAGE, "
					+ "FUNCTION, GUARDRAIL, MODEL). When supplied, the classification step is skipped.";
		} else if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "Optional model engine id used to reason about the prompt. Defaults to the user's configured "
					+ "text-generation model, then to the first one they can access.";
		} else if (key.equals(ReactorKeysEnum.LIMIT.getKey())) {
			return "Optional cap on how many templates are considered. Clamped to " + MAX_TEMPLATE_FETCH + ".";
		}
		return super.getDescriptionForKey(key);
	}

	@Override
	public JSONObject getResponseSchema() {
		JSONObject properties = new JSONObject();
		properties.put("success", new JSONObject().put("type", "boolean"));
		properties.put("targetKind", new JSONObject().put("type", "string")
				.put("enum", new JSONArray().put("PROJECT").put("ENGINE"))
				.put("description", "Whether the plan creates a project or an engine"));
		properties.put("targetType", new JSONObject().put("type", "string")
				.put("enum", toJsonArray(PROJECT_TARGETS, ENGINE_TARGETS))
				.put("description", "The specific project or engine type to create"));
		properties.put("targetTypeSource", new JSONObject().put("type", "string")
				.put("enum", new JSONArray().put("client").put("model").put("default")));
		properties.put("projectName", new JSONObject().put("type", "string")
				.put("description", "Suggested name; the caller is responsible for de-duplicating it"));
		properties.put("templateProjectId", new JSONObject().put("type", new JSONArray().put("string").put("null"))
				.put("description", "Template project to clone; null means start empty. Always null for engines"));
		properties.put("templateProjectName",
				new JSONObject().put("type", new JSONArray().put("string").put("null")));
		properties.put("templateProjectType",
				new JSONObject().put("type", new JSONArray().put("string").put("null")));
		properties.put("templateMatchConfidence", new JSONObject().put("type", "string")
				.put("enum", new JSONArray().put("HIGH").put("MEDIUM").put("LOW").put("NONE")));
		properties.put("useBlankApp", new JSONObject().put("type", "boolean")
				.put("description", "True when no template was matched"));
		properties.put("agentProjectId", new JSONObject().put("type", "string").put("description",
				"WORKSPACE project id to run the assistant under; defaults to " + Constants.AGENT_APP_BUILDER));
		properties.put("agentProjectName", new JSONObject().put("type", new JSONArray().put("string").put("null")));
		properties.put("agentIsDefault", new JSONObject().put("type", "boolean"));
		properties.put("engineId",
				new JSONObject().put("type", "string").put("description", "Model that produced the plan"));
		properties.put("templateCandidateCount", new JSONObject().put("type", "integer"));
		properties.put("agentCandidateCount", new JSONObject().put("type", "integer"));
		properties.put("degraded", new JSONObject().put("type", "array")
				.put("items", new JSONObject().put("type", "string"))
				.put("description", "Steps that fell back; empty when everything resolved from the model"));
		properties.put("error", new JSONObject().put("type", "string")
				.put("description", "Present only when success is false"));

		JSONObject schema = new JSONObject();
		schema.put("type", "object");
		schema.put("description", "Plan for creating a project or engine from a natural-language prompt");
		schema.put("properties", properties);
		return schema;
	}

	private static JSONArray toJsonArray(Collection<String> first, Collection<String> second) {
		JSONArray array = new JSONArray();
		for (String value : first) {
			array.put(value);
		}
		for (String value : second) {
			array.put(value);
		}
		return array;
	}
}
