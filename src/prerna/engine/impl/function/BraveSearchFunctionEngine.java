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
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import prerna.engine.api.FunctionTypeEnum;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Function engine that runs a web search against the Brave Search API and hands
 * back a compact list of {title, url, snippet} results.
 *
 * <p>
 * The normalized output is the point. A raw payload carries ranking metadata,
 * profile blocks, and thumbnail objects that a model has to read past before it
 * reaches the text that answers the question, so this engine strips the
 * response down to the fields worth spending context on. That is what makes it
 * usable as a drop in web search for a model that has none of its own.
 *
 * <p>
 * Every search knob is a per call parameter with an SMSS level default behind
 * it, so an admin can pin a country and result count once and callers only have
 * to pass a query.
 */
public class BraveSearchFunctionEngine extends AbstractFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(BraveSearchFunctionEngine.class);

	private static String API_KEY_KEY = "API_KEY";
	private static String ENDPOINT_KEY = "ENDPOINT";
	private static String COUNT_KEY = "COUNT";
	private static String COUNTRY_KEY = "COUNTRY";
	private static String SEARCH_LANGUAGE_KEY = "SEARCH_LANGUAGE";
	private static String UI_LANGUAGE_KEY = "UI_LANGUAGE";
	private static String SAFE_SEARCH_KEY = "SAFE_SEARCH";
	private static String FRESHNESS_KEY = "FRESHNESS";
	private static String EXTRA_SNIPPETS_KEY = "EXTRA_SNIPPETS";
	private static String SNIPPET_LENGTH_KEY = "SNIPPET_LENGTH";

	// the public brave endpoint. a proxy that mirrors the same route sets ENDPOINT
	// in the SMSS instead
	private static String DEFAULT_ENDPOINT = "https://api.search.brave.com/res/v1/web/search";

	// brave authenticates on this header rather than an authorization header
	private static String SUBSCRIPTION_TOKEN_HEADER = "X-Subscription-Token";

	// brave rejects a count above this, so a caller asking for more is clamped
	// rather than sent through and failed
	private static int MAX_COUNT = 20;

	// brave's offset is a page index rather than a result index, and it stops at
	// the tenth page
	private static int MAX_PAGE = 9;

	// parameters execute understands. anything else a caller passes is ignored
	// rather than folded into the query string, since brave rejects an unknown
	// query parameter outright
	private static String QUERY_PARAM = "query";
	private static String LIMIT_PARAM = "limit";
	private static String PAGE_PARAM = "page";
	private static String COUNTRY_PARAM = "country";
	private static String FRESHNESS_PARAM = "freshness";
	private static String SAFE_SEARCH_PARAM = "safeSearch";

	// keys on the brave response
	private static String WEB_KEY = "web";
	private static String RESULTS_KEY = "results";
	private static String QUERY_KEY = "query";
	private static String ALTERED_KEY = "altered";
	private static String MORE_RESULTS_KEY = "more_results_available";
	private static String EXTRA_SNIPPETS_RESPONSE_KEY = "extra_snippets";

	private String apiKey = null;
	private String endpoint = DEFAULT_ENDPOINT;

	// search defaults, all overridable per execute call
	private int count = 5;
	private String country = null;
	private String searchLanguage = null;
	private String uiLanguage = null;
	private String safeSearch = "moderate";
	private String freshness = null;

	// brave can return up to five additional excerpts per result. they are extra
	// grounding when a single description is too thin, and extra tokens when it
	// is not, so this is off unless an admin asks for it
	private boolean extraSnippets = false;

	// 0 means hand back whatever brave returned. a positive value trims each
	// snippet so a wide search cannot blow out a model's context
	private int snippetLength = 0;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.apiKey = smssProp.getProperty(API_KEY_KEY);
		if (StringUtils.isEmpty(this.apiKey)) {
			throw new IllegalArgumentException("Must have key " + API_KEY_KEY + " in SMSS");
		}

		// the UI writes a blank line for every optional field left empty, so an
		// unset key arrives as "" rather than absent - defaultIfEmpty covers both
		this.endpoint = StringUtils.defaultIfEmpty(smssProp.getProperty(ENDPOINT_KEY), this.endpoint);
		// the query string is appended with a ?, so a trailing slash would produce a
		// path that no longer matches the search route
		while (this.endpoint.endsWith("/")) {
			this.endpoint = this.endpoint.substring(0, this.endpoint.length() - 1);
		}
		Utility.checkIfValidDomain(this.endpoint);

		this.count = clampCount(NumberUtils.toInt(smssProp.getProperty(COUNT_KEY), this.count));
		this.snippetLength = Math.max(0,
				NumberUtils.toInt(smssProp.getProperty(SNIPPET_LENGTH_KEY), this.snippetLength));

		this.country = StringUtils.defaultIfEmpty(smssProp.getProperty(COUNTRY_KEY), this.country);
		this.searchLanguage = StringUtils.defaultIfEmpty(smssProp.getProperty(SEARCH_LANGUAGE_KEY),
				this.searchLanguage);
		this.uiLanguage = StringUtils.defaultIfEmpty(smssProp.getProperty(UI_LANGUAGE_KEY), this.uiLanguage);
		this.safeSearch = StringUtils.defaultIfEmpty(smssProp.getProperty(SAFE_SEARCH_KEY), this.safeSearch);
		this.freshness = StringUtils.defaultIfEmpty(smssProp.getProperty(FRESHNESS_KEY), this.freshness);
		this.extraSnippets = Boolean.parseBoolean(smssProp.getProperty(EXTRA_SNIPPETS_KEY));

		// the SMSS does not have to spell out the function metadata since we know
		// what execute supports. anything defined in the SMSS wins
		setDefaultFunctionMetadata();
	}

	/**
	 * Fill in the function metadata that the SMSS did not define. The parameters
	 * describe what {@link #execute(Map)} understands, and their descriptions carry
	 * the defaults this engine was opened with so a caller knows what it gets when
	 * it leaves one out. A value set in the SMSS is never overwritten.
	 */
	private void setDefaultFunctionMetadata() {
		if (this.functionDescription == null || this.functionDescription.isEmpty()) {
			this.functionDescription = """
					Search the public web and return the top results as a list of {title, url, snippet}. \
					Use this to answer questions about current events, recent releases, prices, or anything \
					else that postdates or falls outside what the model already knows. Read the snippets to \
					answer and cite the urls used.\
					""";
		}

		if (this.parameters == null || this.parameters.isEmpty()) {
			List<FunctionParameter> defaultParameters = new ArrayList<>();
			defaultParameters.add(new FunctionParameter(QUERY_PARAM, "string", """
					The search terms. Phrase this the way a person would type it into a search box rather \
					than as a full sentence question.\
					"""));
			defaultParameters.add(new FunctionParameter(LIMIT_PARAM, "integer", """
					Optional number of results to return, up to %d. Defaults to %d.\
					""".formatted(MAX_COUNT, this.count)));
			defaultParameters.add(new FunctionParameter(PAGE_PARAM, "integer", """
					Optional zero based page of results, up to %d. Each page holds as many results as the \
					limit, so page 1 with a limit of 5 returns the sixth through tenth results. Use this \
					only when the first page did not answer the question.\
					""".formatted(MAX_PAGE)));
			defaultParameters.add(new FunctionParameter(COUNTRY_PARAM, "string", """
					Optional two letter country code to search from, ie US or GB.\
					""" + defaultText(this.country)));
			defaultParameters.add(new FunctionParameter(FRESHNESS_PARAM, "string", """
					Optional recency filter. Use pd for the last day, pw for the last week, pm for the last \
					month, py for the last year, or a YYYY-MM-DDtoYYYY-MM-DD date range.\
					""" + defaultText(this.freshness)));
			defaultParameters.add(new FunctionParameter(SAFE_SEARCH_PARAM, "string", """
					Optional adult content filter. Use off, moderate, or strict.\
					""" + defaultText(this.safeSearch)));
			this.parameters = defaultParameters;
		}

		if (this.requiredParameters == null || this.requiredParameters.isEmpty()) {
			this.requiredParameters = new ArrayList<>(Arrays.asList(QUERY_PARAM));
		}
	}

	/**
	 * Hold a requested result count inside what a single search accepts.
	 *
	 * @param requestedCount the count asked for
	 * @return the count to actually send
	 */
	private static int clampCount(int requestedCount) {
		if (requestedCount < 1) {
			return 1;
		}
		if (requestedCount > MAX_COUNT) {
			return MAX_COUNT;
		}
		return requestedCount;
	}

	/**
	 * Run a web search and return the results the caller can actually use.
	 *
	 * @param parameterValues the runtime parameters for this call
	 * @return a map of {query, alteredQuery, moreResultsAvailable, results}, or the
	 *         raw response when it holds no web results block to unwrap
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

		String url = buildSearchUrl(query, parameterValues);
		classLogger.info("Running a brave web search for '{}' against {}", query, this.endpoint);

		Map<String, String> headers = new HashMap<>();
		headers.put(SUBSCRIPTION_TOKEN_HEADER, this.apiKey);
		headers.put(HttpHeaders.ACCEPT, "application/json");
		String response = HttpHelperUtility.getRequest(url, headers, null, null, null);

		if (response == null || response.trim().isEmpty()) {
			return emptyResult(query);
		}
		return parseResponse(query, response);
	}

	/**
	 * Build the full search url for a single execute call, applying the SMSS
	 * defaults for anything the caller left out.
	 *
	 * @param query           the search terms
	 * @param parameterValues the runtime parameters for this call
	 * @return the fully constructed request url
	 */
	private String buildSearchUrl(String query, Map<String, Object> parameterValues) {
		Map<String, String> queryParams = new LinkedHashMap<>();
		queryParams.put("q", query);

		int runTimeCount = getIntParameterValue(parameterValues, LIMIT_PARAM, this.count);
		if (runTimeCount > MAX_COUNT) {
			classLogger.warn("A limit of {} was asked for but a single search is capped at {}", runTimeCount,
					MAX_COUNT);
		}
		queryParams.put("count", Integer.toString(clampCount(runTimeCount)));

		int runTimePage = getIntParameterValue(parameterValues, PAGE_PARAM, 0);
		if (runTimePage > MAX_PAGE) {
			classLogger.warn("Page {} was asked for but paging stops at page {}", runTimePage, MAX_PAGE);
			runTimePage = MAX_PAGE;
		}
		if (runTimePage > 0) {
			queryParams.put("offset", Integer.toString(runTimePage));
		}

		String runTimeCountry = getParameterValue(parameterValues, COUNTRY_PARAM, this.country);
		if (runTimeCountry != null) {
			queryParams.put("country", runTimeCountry.toUpperCase());
		}
		if (this.searchLanguage != null) {
			queryParams.put("search_lang", this.searchLanguage.toLowerCase());
		}
		if (this.uiLanguage != null) {
			queryParams.put("ui_lang", this.uiLanguage);
		}
		String runTimeSafeSearch = getParameterValue(parameterValues, SAFE_SEARCH_PARAM, this.safeSearch);
		if (runTimeSafeSearch != null) {
			queryParams.put("safesearch", runTimeSafeSearch.toLowerCase());
		}
		String runTimeFreshness = getParameterValue(parameterValues, FRESHNESS_PARAM, this.freshness);
		if (runTimeFreshness != null) {
			queryParams.put("freshness", runTimeFreshness);
		}
		if (this.extraSnippets) {
			queryParams.put("extra_snippets", "true");
		}

		StringBuilder url = new StringBuilder(this.endpoint);
		boolean first = true;
		for (String paramKey : queryParams.keySet()) {
			url.append(first ? "?" : "&");
			url.append(paramKey).append("=")
					.append(URLEncoder.encode(queryParams.get(paramKey), StandardCharsets.UTF_8));
			first = false;
		}
		return url.toString();
	}

	/**
	 * Strip the payload down to the fields a caller needs. A spelling correction
	 * that was applied is passed through under {@code alteredQuery} so the caller
	 * can tell that the results answer a slightly different question than the one
	 * asked.
	 *
	 * @param query    the query that produced this response
	 * @param response the raw response body
	 * @return the normalized result map, or the raw response when it holds no web
	 *         results block to unwrap
	 */
	private Object parseResponse(String query, String response) {
		JSONObject root = null;
		try {
			root = new JSONObject(response);
		} catch (Exception e) {
			classLogger.warn("Could not parse the brave response for '{}', returning it as is", query, e);
			return response;
		}

		JSONObject web = root.optJSONObject(WEB_KEY);
		if (web == null) {
			// no web block at all. that is either a genuine zero result search or a
			// shape this engine does not know, and the caller can tell those apart
			// from the raw body better than a guess here can
			classLogger.info("No {} block in the brave response for '{}'", WEB_KEY, query);
			return response;
		}

		List<Map<String, Object>> results = new ArrayList<>();
		JSONArray values = web.optJSONArray(RESULTS_KEY);
		if (values != null) {
			for (int i = 0; i < values.length(); i++) {
				JSONObject item = values.optJSONObject(i);
				if (item == null) {
					continue;
				}
				Map<String, Object> result = new LinkedHashMap<>();
				result.put("title", item.optString("title", null));
				result.put("url", item.optString("url", null));
				// brave calls the summary text "description"
				result.put("snippet", trimSnippet(item.optString("description", null)));
				putIfPresent(result, "pageAge", item.optString("page_age", null));
				List<String> additional = readExtraSnippets(item);
				if (!additional.isEmpty()) {
					result.put("extraSnippets", additional);
				}
				results.add(result);
			}
		}

		Map<String, Object> output = new LinkedHashMap<>();
		output.put("query", query);
		JSONObject queryContext = root.optJSONObject(QUERY_KEY);
		if (queryContext != null) {
			putIfPresent(output, "alteredQuery", queryContext.optString(ALTERED_KEY, null));
		}
		if (web.has(MORE_RESULTS_KEY)) {
			output.put("moreResultsAvailable", web.optBoolean(MORE_RESULTS_KEY));
		}
		output.put("results", results);
		return output;
	}

	/**
	 * Read the additional excerpts off a result, trimming each the same way the
	 * main snippet is trimmed.
	 *
	 * @param item a single search result
	 * @return the extra excerpts, empty when there are none
	 */
	private List<String> readExtraSnippets(JSONObject item) {
		List<String> additional = new ArrayList<>();
		JSONArray extra = item.optJSONArray(EXTRA_SNIPPETS_RESPONSE_KEY);
		if (extra == null) {
			return additional;
		}
		for (int i = 0; i < extra.length(); i++) {
			String snippet = extra.optString(i, null);
			if (snippet != null && !snippet.trim().isEmpty()) {
				additional.add(trimSnippet(snippet));
			}
		}
		return additional;
	}

	/**
	 * Cut a snippet down to {@link #SNIPPET_LENGTH_KEY} characters when a limit is
	 * configured, breaking on the last whole word so the text does not end mid
	 * word.
	 *
	 * @param snippet the snippet that came back
	 * @return the snippet, trimmed when it is over the configured limit
	 */
	private String trimSnippet(String snippet) {
		if (snippet == null || this.snippetLength < 1 || snippet.length() <= this.snippetLength) {
			return snippet;
		}
		String trimmed = snippet.substring(0, this.snippetLength);
		int lastSpace = trimmed.lastIndexOf(' ');
		if (lastSpace > 0) {
			trimmed = trimmed.substring(0, lastSpace);
		}
		return trimmed + "...";
	}

	/**
	 * Build the payload for a search that came back with nothing, so a caller gets
	 * the same shape whether or not there were hits.
	 *
	 * @param query the query that returned nothing
	 * @return a result map with an empty result list
	 */
	private static Map<String, Object> emptyResult(String query) {
		Map<String, Object> output = new LinkedHashMap<>();
		output.put("query", query);
		output.put("results", new ArrayList<>());
		return output;
	}

	/**
	 * Add a value to the output only when one actually came back, so optional
	 * fields do not show up as nulls the caller has to read past.
	 *
	 * @param map   the map being built
	 * @param key   the key to set
	 * @param value the value that came back, possibly null or empty
	 */
	private static void putIfPresent(Map<String, Object> map, String key, String value) {
		if (value != null && !value.trim().isEmpty()) {
			map.put(key, value);
		}
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.BRAVE_SEARCH.name();
	}

	@Override
	public void close() throws IOException {
		// nothing is held open between searches
	}

	/**
	 * Exposed for the reactor layer so a tool can advertise how many results it is
	 * allowed to ask for.
	 *
	 * @return the largest limit a single search accepts
	 */
	public static int getMaxCount() {
		return MAX_COUNT;
	}

}
