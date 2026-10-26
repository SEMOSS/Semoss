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
package prerna.engine.impl.function;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.http.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.engine.api.FunctionTypeEnum;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Function engine that reaches the public web through the Foundry
 * {@code web_search} tool on the Azure OpenAI Responses API, which is what
 * replaced the retired Bing Search v7 APIs.
 *
 * <p>
 * Foundry does not hand back search results. A model deployment reads the web
 * and answers, and what comes back is grounded prose plus {@code url_citation}
 * annotations, so this engine returns
 * {@code {query, answer, citations, searches}} rather than a result list. Use
 * it when the caller wants a written answer it can quote and attribute.
 *
 * <p>
 * Two consequences of that design worth knowing before wiring this up. A search
 * here invokes an LLM, so it costs a model call plus a grounding tool call. And
 * because the model decides whether to search at all, the request is built to
 * push it toward doing so - see {@link #INSTRUCTION_KEY}.
 *
 * <p>
 * Domain allow and block lists are SMSS level only, never per call. Which
 * sources an agent is permitted to ground on is a governance decision an admin
 * makes, not a knob the calling model should be able to widen for itself.
 */
public class BingSearchFunctionEngine extends AbstractFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(BingSearchFunctionEngine.class);

	private static String API_KEY_KEY = "API_KEY";
	private static String ENDPOINT_KEY = "ENDPOINT";
	private static String MODEL_KEY = "MODEL";
	private static String AUTH_TYPE_KEY = "AUTH_TYPE";
	private static String INSTRUCTION_KEY = "INSTRUCTION";
	private static String REASONING_EFFORT_KEY = "REASONING_EFFORT";
	private static String ALLOWED_DOMAINS_KEY = "ALLOWED_DOMAINS";
	private static String BLOCKED_DOMAINS_KEY = "BLOCKED_DOMAINS";
	private static String SEARCH_CONTEXT_SIZE_KEY = "SEARCH_CONTEXT_SIZE";
	private static String COUNTRY_KEY = "COUNTRY";
	private static String CITY_KEY = "CITY";
	private static String REGION_KEY = "REGION";
	private static String TIMEZONE_KEY = "TIMEZONE";

	// api key auth sends the key on this header, entra id sends a bearer token on
	// the standard authorization header
	private static String API_KEY_AUTH = "api_key";
	private static String ENTRA_AUTH = "entra";
	private static String API_KEY_HEADER = "api-key";

	// the responses route hangs off the resource's v1 base path. an admin pastes
	// whichever form is in front of them, so both are accepted
	private static String RESPONSES_PATH = "/openai/v1/responses";

	// the domain filter is capped at this many entries
	private static int MAX_DOMAINS = 100;

	// parameters execute understands
	private static String QUERY_PARAM = "query";
	private static String COUNTRY_PARAM = "country";

	// keys on the responses payload
	private static String OUTPUT_KEY = "output";
	private static String TYPE_KEY = "type";
	private static String MESSAGE_TYPE = "message";
	private static String WEB_SEARCH_CALL_TYPE = "web_search_call";
	private static String CONTENT_KEY = "content";
	private static String TEXT_KEY = "text";
	private static String ANNOTATIONS_KEY = "annotations";
	private static String URL_CITATION_TYPE = "url_citation";
	private static String ACTION_KEY = "action";
	private static String ERROR_KEY = "error";

	private String apiKey = null;
	private String endpoint = null;
	private String model = null;
	private String authType = API_KEY_AUTH;

	// the model decides whether to call the search tool, so the query is wrapped
	// in an instruction that tells it to go look rather than answer from memory
	private String instruction = """
			Search the web and answer the following, citing the sources you used. If the web does not \
			answer it, say so rather than guessing.\
			""";

	private String reasoningEffort = null;
	private String searchContextSize = null;
	private List<String> allowedDomains = null;
	private List<String> blockedDomains = null;

	// approximate user location, all optional
	private String country = null;
	private String city = null;
	private String region = null;
	private String timezone = null;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.apiKey = smssProp.getProperty(API_KEY_KEY);
		if (StringUtils.isEmpty(this.apiKey)) {
			throw new IllegalArgumentException("Must have key " + API_KEY_KEY + " in SMSS");
		}

		this.endpoint = smssProp.getProperty(ENDPOINT_KEY);
		if (StringUtils.isEmpty(this.endpoint)) {
			throw new IllegalArgumentException("Must have key " + ENDPOINT_KEY + " in SMSS");
		}
		this.endpoint = normalizeEndpoint(this.endpoint);
		Utility.checkIfValidDomain(this.endpoint);

		this.model = smssProp.getProperty(MODEL_KEY);
		if (StringUtils.isEmpty(this.model)) {
			throw new IllegalArgumentException(
					"Must have key " + MODEL_KEY + " in SMSS, set to the name of the model deployment to search with");
		}

		// the UI writes a blank line for every optional field left empty, so an
		// unset key arrives as "" rather than absent - defaultIfEmpty covers both
		this.authType = StringUtils.defaultIfEmpty(smssProp.getProperty(AUTH_TYPE_KEY), this.authType).toLowerCase();
		if (!this.authType.equals(API_KEY_AUTH) && !this.authType.equals(ENTRA_AUTH)) {
			throw new IllegalArgumentException("BingSearchFunctionEngine only supports " + API_KEY_AUTH + " or "
					+ ENTRA_AUTH + " for the " + AUTH_TYPE_KEY + " key");
		}

		this.instruction = StringUtils.defaultIfEmpty(smssProp.getProperty(INSTRUCTION_KEY), this.instruction);
		this.reasoningEffort = StringUtils.defaultIfEmpty(smssProp.getProperty(REASONING_EFFORT_KEY),
				this.reasoningEffort);
		this.searchContextSize = StringUtils.defaultIfEmpty(smssProp.getProperty(SEARCH_CONTEXT_SIZE_KEY),
				this.searchContextSize);

		this.allowedDomains = readDomainList(smssProp, ALLOWED_DOMAINS_KEY);
		this.blockedDomains = readDomainList(smssProp, BLOCKED_DOMAINS_KEY);

		this.country = StringUtils.defaultIfEmpty(smssProp.getProperty(COUNTRY_KEY), this.country);
		this.city = StringUtils.defaultIfEmpty(smssProp.getProperty(CITY_KEY), this.city);
		this.region = StringUtils.defaultIfEmpty(smssProp.getProperty(REGION_KEY), this.region);
		this.timezone = StringUtils.defaultIfEmpty(smssProp.getProperty(TIMEZONE_KEY), this.timezone);

		// the SMSS does not have to spell out the function metadata since we know
		// what execute supports. anything defined in the SMSS wins
		setDefaultFunctionMetadata();
	}

	/**
	 * Put an endpoint into the exact form the request needs, so an admin can paste
	 * whichever one their portal or sample code showed them. All three of
	 * {@code https://x.openai.azure.com}, {@code .../openai/v1}, and
	 * {@code .../openai/v1/responses} land on the same place.
	 *
	 * @param configuredEndpoint the endpoint as it was configured
	 * @return the full responses url
	 */
	private static String normalizeEndpoint(String configuredEndpoint) {
		while (configuredEndpoint.endsWith("/")) {
			configuredEndpoint = configuredEndpoint.substring(0, configuredEndpoint.length() - 1);
		}
		if (configuredEndpoint.endsWith(RESPONSES_PATH)) {
			return configuredEndpoint;
		}
		if (configuredEndpoint.endsWith("/openai/v1")) {
			return configuredEndpoint + "/responses";
		}
		return configuredEndpoint + RESPONSES_PATH;
	}

	/**
	 * Read a comma separated domain list off the SMSS, dropping any http prefix
	 * since the filter wants a bare host.
	 *
	 * @param smssProp the engine SMSS properties
	 * @param key      the SMSS key to read
	 * @return the domains, or null when the key is not set
	 */
	private static List<String> readDomainList(Properties smssProp, String key) {
		String value = smssProp.getProperty(key);
		if (StringUtils.isEmpty(value)) {
			return null;
		}
		List<String> domains = new ArrayList<>();
		for (String domain : value.split(",")) {
			if ((domain = domain.trim()).isEmpty()) {
				continue;
			}
			domain = domain.replaceFirst("^[Hh][Tt][Tt][Pp][Ss]?://", "");
			while (domain.endsWith("/")) {
				domain = domain.substring(0, domain.length() - 1);
			}
			if (!domain.isEmpty()) {
				domains.add(domain);
			}
		}
		if (domains.isEmpty()) {
			return null;
		}
		if (domains.size() > MAX_DOMAINS) {
			classLogger.warn("{} holds {} domains but the filter accepts {} - the rest are dropped", key,
					domains.size(), MAX_DOMAINS);
			domains = domains.subList(0, MAX_DOMAINS);
		}
		return domains;
	}

	/**
	 * Fill in the function metadata that the SMSS did not define. A value set in
	 * the SMSS is never overwritten.
	 */
	private void setDefaultFunctionMetadata() {
		if (this.functionDescription == null || this.functionDescription.isEmpty()) {
			String description = """
					Search the public web and return a written answer with the urls it was sourced from. \
					Use this for current events, recent releases, prices, or anything else that postdates \
					or falls outside what the model already knows. The answer is already grounded in the \
					cited pages, so quote it and attribute the urls rather than searching again.\
					""";
			if (this.allowedDomains != null) {
				description += " Results are restricted to an approved list of sources.";
			}
			this.functionDescription = description;
		}

		if (this.parameters == null || this.parameters.isEmpty()) {
			List<FunctionParameter> defaultParameters = new ArrayList<>();
			defaultParameters.add(new FunctionParameter(QUERY_PARAM, "string", """
					What to find out. A full question works better here than bare keywords, because a model \
					reads this and decides what to search for.\
					"""));
			defaultParameters.add(new FunctionParameter(COUNTRY_PARAM, "string", """
					Optional two letter country code to search from, ie US or GB.\
					""" + defaultText(this.country)));
			this.parameters = defaultParameters;
		}

		if (this.requiredParameters == null || this.requiredParameters.isEmpty()) {
			this.requiredParameters = new ArrayList<>(Arrays.asList(QUERY_PARAM));
		}
	}

	/**
	 * Run a grounded web search.
	 *
	 * @param parameterValues the runtime parameters for this call
	 * @return a map of {query, answer, citations, searches}, or the raw response
	 *         when it holds no message to unwrap
	 */
	@Override
	public Object execute(Map<String, Object> parameterValues) {
		if (parameterValues == null) {
			parameterValues = new HashMap<>();
		}
		// the insight rides along on the parameter map but is not a search parameter
		parameterValues.remove(Constants.INSIGHT);

		validateRequiredParameters(parameterValues);

		String query = getParameterValue(parameterValues, QUERY_PARAM, null);
		if (query == null) {
			throw new IllegalArgumentException("Must define the " + QUERY_PARAM + " parameter to run a web search");
		}

		String body = buildRequestBody(query, parameterValues);
		classLogger.info("Running a grounded web search for '{}' against {} with deployment {}", query, this.endpoint,
				this.model);

		Map<String, String> headers = new HashMap<>();
		if (this.authType.equals(ENTRA_AUTH)) {
			headers.put("Authorization", "Bearer " + this.apiKey);
		} else {
			headers.put(API_KEY_HEADER, this.apiKey);
		}
		String response = HttpHelperUtility.postRequestStringBody(this.endpoint, headers, body,
				ContentType.APPLICATION_JSON, null, null, null);

		if (response == null || response.trim().isEmpty()) {
			return emptyResult(query);
		}
		return parseResponse(query, response);
	}

	/**
	 * Build the responses request for a single execute call.
	 *
	 * @param query           what the caller wants to find out
	 * @param parameterValues the runtime parameters for this call
	 * @return the json request body
	 */
	private String buildRequestBody(String query, Map<String, Object> parameterValues) {
		JSONObject tool = new JSONObject();
		tool.put("type", "web_search");

		String runTimeCountry = getParameterValue(parameterValues, COUNTRY_PARAM, this.country);
		if (runTimeCountry != null || this.city != null || this.region != null || this.timezone != null) {
			JSONObject userLocation = new JSONObject();
			userLocation.put("type", "approximate");
			if (runTimeCountry != null) {
				userLocation.put("country", runTimeCountry.toUpperCase());
			}
			putIfPresent(userLocation, "city", this.city);
			putIfPresent(userLocation, "region", this.region);
			putIfPresent(userLocation, "timezone", this.timezone);
			tool.put("user_location", userLocation);
		}

		if (this.allowedDomains != null || this.blockedDomains != null) {
			JSONObject filters = new JSONObject();
			if (this.allowedDomains != null) {
				filters.put("allowed_domains", new JSONArray(this.allowedDomains));
			}
			if (this.blockedDomains != null) {
				filters.put("blocked_domains", new JSONArray(this.blockedDomains));
			}
			tool.put("filters", filters);
		}

		if (this.searchContextSize != null) {
			tool.put("search_context_size", this.searchContextSize.toLowerCase());
		}

		JSONObject requestBody = new JSONObject();
		requestBody.put("model", this.model);
		requestBody.put("tools", new JSONArray().put(tool));
		requestBody.put("tool_choice", "auto");
		requestBody.put("input", buildInput(query));
		// ask for the pages that were consulted, not just the ones the answer
		// happened to cite, so the caller can see what the answer rests on
		requestBody.put("include", new JSONArray().put("web_search_call.action.sources"));

		if (this.reasoningEffort != null) {
			requestBody.put("reasoning", new JSONObject().put("effort", this.reasoningEffort.toLowerCase()));
		}

		return requestBody.toString();
	}

	/**
	 * Wrap the caller's query in the instruction that pushes the model to actually
	 * search. Without it a model will happily answer a question it thinks it
	 * already knows, which defeats the point of the tool.
	 *
	 * @param query what the caller wants to find out
	 * @return the input to send
	 */
	private String buildInput(String query) {
		if (this.instruction == null || this.instruction.trim().isEmpty()) {
			return query;
		}
		return this.instruction.trim() + "\n\n" + query;
	}

	/**
	 * Pull the answer, its citations, and the searches that produced it out of the
	 * responses payload.
	 *
	 * <p>
	 * The {@code output} array is walked by {@code type} rather than by position,
	 * because a reasoning model adds a {@code reasoning} item alongside the
	 * {@code web_search_call} and {@code message} items and the order is not
	 * promised.
	 *
	 * @param query    the query that produced this response
	 * @param response the raw response body
	 * @return the normalized result map, or the raw response when there is no
	 *         message in it to unwrap
	 */
	private Object parseResponse(String query, String response) {
		JSONObject root = null;
		try {
			root = new JSONObject(response);
		} catch (Exception e) {
			classLogger.warn("Could not parse the foundry response for '{}', returning it as is", query, e);
			return response;
		}

		// a failed call still comes back as json, and the error in it is far more
		// useful to the caller than an empty answer would be
		JSONObject error = root.optJSONObject(ERROR_KEY);
		if (error != null) {
			throw new IllegalArgumentException(
					"The web search failed. Detailed message = " + error.optString("message", error.toString()));
		}

		JSONArray output = root.optJSONArray(OUTPUT_KEY);
		if (output == null) {
			classLogger.info("No {} array in the foundry response for '{}'", OUTPUT_KEY, query);
			return response;
		}

		StringBuilder answer = new StringBuilder();
		List<Map<String, Object>> citations = new ArrayList<>();
		List<String> searches = new ArrayList<>();
		Set<String> seenCitations = new HashSet<>();

		for (int i = 0; i < output.length(); i++) {
			JSONObject item = output.optJSONObject(i);
			if (item == null) {
				continue;
			}
			String itemType = item.optString(TYPE_KEY, null);
			if (WEB_SEARCH_CALL_TYPE.equals(itemType)) {
				readSearchCall(item, searches, citations, seenCitations);
			} else if (MESSAGE_TYPE.equals(itemType)) {
				readMessage(item, answer, citations, seenCitations);
			}
		}

		if (answer.length() == 0) {
			classLogger.info("No {} item in the foundry response for '{}'", MESSAGE_TYPE, query);
			return response;
		}

		Map<String, Object> result = new LinkedHashMap<>();
		result.put("query", query);
		result.put("answer", answer.toString());
		result.put("citations", citations);
		if (!searches.isEmpty()) {
			result.put("searches", searches);
		}
		// the model answering from memory rather than searching is a real outcome to
		// surface, since the answer is then no better grounded than the caller's own
		result.put("searchPerformed", !searches.isEmpty());
		return result;
	}

	/**
	 * Read the query a search call ran, and the sources it turned up.
	 *
	 * @param item          a {@code web_search_call} output item
	 * @param searches      collects the queries that were run
	 * @param citations     collects the sources
	 * @param seenCitations urls already collected, so a page consulted and then
	 *                      cited is only listed once
	 */
	private static void readSearchCall(JSONObject item, List<String> searches, List<Map<String, Object>> citations,
			Set<String> seenCitations) {
		JSONObject action = item.optJSONObject(ACTION_KEY);
		if (action == null) {
			return;
		}
		String searchQuery = action.optString("query", null);
		if (searchQuery != null && !searchQuery.trim().isEmpty() && !searches.contains(searchQuery)) {
			searches.add(searchQuery);
		}
		JSONArray sources = action.optJSONArray("sources");
		if (sources == null) {
			return;
		}
		for (int i = 0; i < sources.length(); i++) {
			JSONObject source = sources.optJSONObject(i);
			if (source == null) {
				continue;
			}
			// action.sources carries no page title, only the url
			addCitation(citations, seenCitations, source.optString("url", null), null);
		}
	}

	/**
	 * Read the grounded text off a message item, along with the url citations
	 * annotated onto it.
	 *
	 * @param item          a {@code message} output item
	 * @param answer        collects the grounded text
	 * @param citations     collects the cited pages
	 * @param seenCitations urls already collected
	 */
	private static void readMessage(JSONObject item, StringBuilder answer, List<Map<String, Object>> citations,
			Set<String> seenCitations) {
		JSONArray content = item.optJSONArray(CONTENT_KEY);
		if (content == null) {
			return;
		}
		for (int i = 0; i < content.length(); i++) {
			JSONObject contentItem = content.optJSONObject(i);
			if (contentItem == null) {
				continue;
			}
			String text = contentItem.optString(TEXT_KEY, null);
			if (text != null && !text.trim().isEmpty()) {
				if (answer.length() > 0) {
					answer.append("\n\n");
				}
				answer.append(text);
			}
			JSONArray annotations = contentItem.optJSONArray(ANNOTATIONS_KEY);
			if (annotations == null) {
				continue;
			}
			for (int j = 0; j < annotations.length(); j++) {
				JSONObject annotation = annotations.optJSONObject(j);
				if (annotation == null || !URL_CITATION_TYPE.equals(annotation.optString(TYPE_KEY, null))) {
					continue;
				}
				addCitation(citations, seenCitations, annotation.optString("url", null),
						annotation.optString("title", null));
			}
		}
	}

	/**
	 * Record a source, filling in a title for one that was already recorded without
	 * it. A page shows up in {@code action.sources} with only a url and again in an
	 * annotation with its title, and the titled version is the more useful of the
	 * two.
	 *
	 * @param citations     the sources collected so far
	 * @param seenCitations urls already collected
	 * @param url           the source url
	 * @param title         the page title, when there is one
	 */
	private static void addCitation(List<Map<String, Object>> citations, Set<String> seenCitations, String url,
			String title) {
		if (url == null || (url = url.trim()).isEmpty()) {
			return;
		}
		boolean hasTitle = title != null && !title.trim().isEmpty();
		if (seenCitations.contains(url)) {
			if (hasTitle) {
				for (Map<String, Object> citation : citations) {
					if (url.equals(citation.get("url")) && !citation.containsKey("title")) {
						citation.put("title", title.trim());
						break;
					}
				}
			}
			return;
		}
		seenCitations.add(url);
		Map<String, Object> citation = new LinkedHashMap<>();
		if (hasTitle) {
			citation.put("title", title.trim());
		}
		citation.put("url", url);
		citations.add(citation);
	}

	/**
	 * Build the payload for a search that came back with nothing, so a caller gets
	 * the same shape whether or not there was an answer.
	 *
	 * @param query the query that returned nothing
	 * @return a result map with an empty answer
	 */
	private static Map<String, Object> emptyResult(String query) {
		Map<String, Object> output = new LinkedHashMap<>();
		output.put("query", query);
		output.put("answer", "");
		output.put("citations", new ArrayList<>());
		output.put("searchPerformed", false);
		return output;
	}

	/**
	 * Set a value on a json object only when there is one, so optional fields are
	 * absent rather than null.
	 *
	 * @param json  the object being built
	 * @param key   the key to set
	 * @param value the value, possibly null or empty
	 */
	private static void putIfPresent(JSONObject json, String key, String value) {
		if (value != null && !value.trim().isEmpty()) {
			json.put(key, value.trim());
		}
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.BING_SEARCH.name();
	}

	@Override
	public void close() throws IOException {
		// nothing is held open between searches
	}

}
