package prerna.engine.impl.servicenow;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.hc.core5.http.HttpHeaders;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

import com.google.gson.reflect.TypeToken;

import prerna.engine.api.FunctionTypeEnum;
import prerna.engine.impl.function.AbstractFunctionEngine;
import prerna.engine.impl.function.FunctionParameter;
import prerna.om.Insight;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.Utility;

public class ServiceNowFunctionEngine extends AbstractFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(ServiceNowFunctionEngine.class);

	private static String ENDPOINT_KEY = "ENDPOINT";

	private static String OAUTH_ENDPOINT_KEY = "OAUTH_ENDPOINT";
	private static String AUTH_TYPE_KEY = "AUTH_TYPE";
	private static String OAUTH_TYPE = "oauth";
	private static String BASIC_TYPE = "basic";
	// basic auth
	private static String BASIC_USERNAME_KEY = "BASIC_USERNAME";
	private static String BASIC_PASSWORD_KEY = "BASIC_PASSWORD";
	// oauth auth
	private static String OAUTH_GRANT_TYPE_KEY = "OAUTH_GRANT_TYPE";
	private static String OAUTH_CLIENT_KEY = "OAUTH_CLIENT";
	private static String OAUTH_SECRET_KEY = "OAUTH_SECRET";

	// table query defaults
	private static String DEFAULT_TABLE_KEY = "DEFAULT_TABLE";
	private static String TABLE_API_PATH_KEY = "TABLE_API_PATH";
	private static String LIMIT_KEY = "LIMIT";
	private static String FIELDS_KEY = "FIELDS";
	private static String DISPLAY_VALUE_KEY = "DISPLAY_VALUE";
	private static String EXCLUDE_REFERENCE_LINK_KEY = "EXCLUDE_REFERENCE_LINK";

	// default oauth token path on a service now instance
	private static String DEFAULT_OAUTH_PATH = "/oauth_token.do";

	// the out of the box table api path. a scoped app exposing its own scripted
	// rest api will have a different path, so this is overridable in the SMSS
	private static String DEFAULT_TABLE_API_PATH = "/api/now/table/";
	// service now wraps the records it returns under this key
	private static String RESULT_KEY = "result";

	// parameters the caller can pass into execute that map onto a sysparm. any
	// parameter that is not one of these is treated as a field filter
	private static String TABLE_PARAM = "table";
	private static String QUERY_PARAM = "query";
	private static String FIELDS_PARAM = "fields";
	private static String LIMIT_PARAM = "limit";
	private static String OFFSET_PARAM = "offset";
	private static String DISPLAY_VALUE_PARAM = "displayValue";
	private static String ORDER_BY_PARAM = "orderBy";
	private static String ORDER_BY_DESC_PARAM = "orderByDesc";

	private static Set<String> RESERVED_PARAMS = new HashSet<>(Arrays.asList(TABLE_PARAM, QUERY_PARAM, FIELDS_PARAM,
			LIMIT_PARAM, OFFSET_PARAM, DISPLAY_VALUE_PARAM, ORDER_BY_PARAM, ORDER_BY_DESC_PARAM));

	// grant types we know how to build a token request for
	private static String CLIENT_CREDENTIALS_GRANT = "client_credentials";
	private static String PASSWORD_GRANT = "password";
	private static String REFRESH_TOKEN_GRANT = "refresh_token";

	// keys on the oauth token response
	private static String ACCESS_TOKEN_RESPONSE_KEY = "access_token";
	private static String REFRESH_TOKEN_RESPONSE_KEY = "refresh_token";
	private static String EXPIRES_IN_RESPONSE_KEY = "expires_in";
	private static String TOKEN_TYPE_RESPONSE_KEY = "token_type";

	// treat the token as expired this many seconds before it actually is so we
	// never send a token that dies in flight
	private static long TOKEN_EXPIRATION_BUFFER_SECONDS = 30;

	private String endpoint = null;
	private String authType = null;

	// table query defaults, all overridable per execute call
	private String defaultTable = null;
	private String tableApiPath = DEFAULT_TABLE_API_PATH;
	private String limit = "25";
	private String fields = null;
	private String displayValue = "true";
	private String excludeReferenceLink = "true";

	private String basicAuthUsername = null;
	private String basicAuthPassword = null;

	private String oauthEndpoint = null;
	private String grant_type = "client_credentials";
	private String oauthClientKey = null;
	private String oauthSecretKey = null;
	private long expiresIn = -1;

	// current oauth state
	private String accessToken = null;
	private String tokenType = "Bearer";
	private String refreshToken = null;
	// epoch millis when the current access token should be considered dead
	private long accessTokenExpirationTime = -1;

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.endpoint = smssProp.getProperty(ENDPOINT_KEY);
		if (this.endpoint == null || this.endpoint.isEmpty()) {
			throw new IllegalArgumentException("Must have key " + ENDPOINT_KEY + " in SMSS");
		}
		// drop the trailing slash so we can safely append paths
		while (this.endpoint.endsWith("/")) {
			this.endpoint = this.endpoint.substring(0, this.endpoint.length() - 1);
		}
		Utility.checkIfValidDomain(this.endpoint);

		this.authType = smssProp.getProperty(AUTH_TYPE_KEY);
		if (this.authType == null || (this.authType = this.authType.toLowerCase()).isEmpty()) {
			throw new IllegalArgumentException("Must have key " + AUTH_TYPE_KEY + " in SMSS");
		}
		if (!this.authType.equals(OAUTH_TYPE) && !this.authType.equals(BASIC_TYPE)) {
			throw new IllegalArgumentException("ServiceNowFunctionEngine only supports " + BASIC_TYPE + " or "
					+ OAUTH_TYPE + " for the " + AUTH_TYPE_KEY + " key");
		}

		// the username/password are read regardless of the auth type since the
		// oauth password grant needs them as well
		this.basicAuthUsername = smssProp.getProperty(BASIC_USERNAME_KEY);
		if (this.basicAuthUsername != null && this.basicAuthUsername.isEmpty()) {
			this.basicAuthUsername = null;
		}
		this.basicAuthPassword = smssProp.getProperty(BASIC_PASSWORD_KEY);
		if (this.basicAuthPassword != null && this.basicAuthPassword.isEmpty()) {
			this.basicAuthPassword = null;
		}

		if (this.authType.equals(BASIC_TYPE)) {
			if (this.basicAuthUsername == null) {
				throw new IllegalArgumentException(
						"Must have key " + BASIC_USERNAME_KEY + " in SMSS when " + AUTH_TYPE_KEY + " is " + BASIC_TYPE);
			}
			if (this.basicAuthPassword == null) {
				throw new IllegalArgumentException(
						"Must have key " + BASIC_PASSWORD_KEY + " in SMSS when " + AUTH_TYPE_KEY + " is " + BASIC_TYPE);
			}
		} else {
			this.oauthClientKey = smssProp.getProperty(OAUTH_CLIENT_KEY);
			if (this.oauthClientKey == null || this.oauthClientKey.isEmpty()) {
				throw new IllegalArgumentException(
						"Must have key " + OAUTH_CLIENT_KEY + " in SMSS when " + AUTH_TYPE_KEY + " is " + OAUTH_TYPE);
			}
			this.oauthSecretKey = smssProp.getProperty(OAUTH_SECRET_KEY);
			if (this.oauthSecretKey == null || this.oauthSecretKey.isEmpty()) {
				throw new IllegalArgumentException(
						"Must have key " + OAUTH_SECRET_KEY + " in SMSS when " + AUTH_TYPE_KEY + " is " + OAUTH_TYPE);
			}

			String smssGrantType = smssProp.getProperty(OAUTH_GRANT_TYPE_KEY);
			if (smssGrantType != null && !(smssGrantType = smssGrantType.toLowerCase()).isEmpty()) {
				this.grant_type = smssGrantType;
			}
			if (!this.grant_type.equals(CLIENT_CREDENTIALS_GRANT) && !this.grant_type.equals(PASSWORD_GRANT)) {
				throw new IllegalArgumentException("ServiceNowFunctionEngine only supports " + CLIENT_CREDENTIALS_GRANT
						+ " or " + PASSWORD_GRANT + " for the " + OAUTH_GRANT_TYPE_KEY + " key");
			}
			if (this.grant_type.equals(PASSWORD_GRANT)) {
				if (this.basicAuthUsername == null) {
					throw new IllegalArgumentException("Must have key " + BASIC_USERNAME_KEY + " in SMSS when "
							+ OAUTH_GRANT_TYPE_KEY + " is " + PASSWORD_GRANT);
				}
				if (this.basicAuthPassword == null) {
					throw new IllegalArgumentException("Must have key " + BASIC_PASSWORD_KEY + " in SMSS when "
							+ OAUTH_GRANT_TYPE_KEY + " is " + PASSWORD_GRANT);
				}
			}

			this.oauthEndpoint = smssProp.getProperty(OAUTH_ENDPOINT_KEY);
			if (this.oauthEndpoint == null || this.oauthEndpoint.isEmpty()) {
				// default to the standard token endpoint on the instance
				this.oauthEndpoint = this.endpoint + DEFAULT_OAUTH_PATH;
			}
			Utility.checkIfValidDomain(this.oauthEndpoint);
		}

		// all of the table query settings are optional defaults that the caller can
		// override on a per execute basis
		this.defaultTable = getProperty(smssProp, DEFAULT_TABLE_KEY, this.defaultTable);
		this.tableApiPath = getProperty(smssProp, TABLE_API_PATH_KEY, this.tableApiPath);
		// the path is concatenated between the endpoint and the table name, so
		// normalize both ends to exactly one slash
		if (!this.tableApiPath.startsWith("/")) {
			this.tableApiPath = "/" + this.tableApiPath;
		}
		if (!this.tableApiPath.endsWith("/")) {
			this.tableApiPath = this.tableApiPath + "/";
		}
		this.limit = getProperty(smssProp, LIMIT_KEY, this.limit);
		this.fields = getProperty(smssProp, FIELDS_KEY, this.fields);
		this.displayValue = getProperty(smssProp, DISPLAY_VALUE_KEY, this.displayValue);
		this.excludeReferenceLink = getProperty(smssProp, EXCLUDE_REFERENCE_LINK_KEY, this.excludeReferenceLink);

		// the SMSS does not have to spell out the function metadata since we know
		// what execute supports. anything defined in the SMSS wins
		setDefaultFunctionMetadata();
	}

	/**
	 * Fill in the function metadata that the SMSS did not define. The parameters
	 * describe the sysparms {@link #execute(Map)} understands, and their
	 * descriptions carry the defaults this engine was opened with so a caller knows
	 * what it gets when it leaves one out. A value set in the SMSS is never
	 * overwritten.
	 */
	private void setDefaultFunctionMetadata() {
		if (this.functionDescription == null || this.functionDescription.isEmpty()) {
			this.functionDescription = "Query records from the "
					+ (this.defaultTable != null ? this.defaultTable + " " : "") + "service now table at "
					+ this.endpoint
					+ ". Filters can be passed as a service now encoded query or as individual field values.";
		}

		if (this.parameters == null || this.parameters.isEmpty()) {
			List<FunctionParameter> defaultParameters = new ArrayList<>();
			// only worth asking for the table when there is no default to fall back on
			if (this.defaultTable == null) {
				defaultParameters.add(new FunctionParameter(TABLE_PARAM, "string",
						"The name of the service now table to query, ie incident or sc_request."));
			}
			defaultParameters.add(new FunctionParameter(QUERY_PARAM, "string",
					"Optional service now encoded query used to filter the records. Multiple clauses are joined with"
							+ " a ^, ie active=true^priority=1. Any field level filter can also be passed as its own"
							+ " parameter instead."));
			defaultParameters.add(new FunctionParameter(FIELDS_PARAM, "string",
					"Optional comma separated list of fields to return on each record. Returns every field when not"
							+ " set." + defaultText(this.fields)));
			defaultParameters.add(new FunctionParameter(LIMIT_PARAM, "integer",
					"Optional maximum number of records to return." + defaultText(this.limit)));
			defaultParameters.add(new FunctionParameter(OFFSET_PARAM, "integer",
					"Optional number of records to skip before returning results, used to page through a large table."));
			defaultParameters.add(new FunctionParameter(DISPLAY_VALUE_PARAM, "string",
					"Optional control over how reference and choice fields come back. Use true for the readable"
							+ " display value, false for the raw database value, or all for both."
							+ defaultText(this.displayValue)));
			defaultParameters.add(new FunctionParameter(ORDER_BY_PARAM, "string",
					"Optional field name to sort the records by in ascending order."));
			defaultParameters.add(new FunctionParameter(ORDER_BY_DESC_PARAM, "string",
					"Optional field name to sort the records by in descending order."));
			this.parameters = defaultParameters;
		}

		if (this.requiredParameters == null || this.requiredParameters.isEmpty()) {
			List<String> defaultRequiredParameters = new ArrayList<>();
			// every sysparm is optional, so the table is the only thing we need and
			// only when the SMSS did not set a default
			if (this.defaultTable == null) {
				defaultRequiredParameters.add(TABLE_PARAM);
			}
			this.requiredParameters = defaultRequiredParameters;
		}
	}

	/**
	 * Build the trailing sentence that tells a caller what value is used when they
	 * leave a parameter out.
	 *
	 * @param defaultValue the default this engine was opened with
	 * @return the sentence to append, or an empty string when there is no default
	 */
	private static String defaultText(String defaultValue) {
		if (defaultValue == null || defaultValue.isEmpty()) {
			return "";
		}
		return " Defaults to " + defaultValue + ".";
	}

	/**
	 * Pull a property off the SMSS, falling back to a default when the key is
	 * missing or empty.
	 *
	 * @param smssProp     the engine SMSS properties
	 * @param key          the SMSS key to read
	 * @param defaultValue value to use when the key is not set
	 * @return the property value or the default
	 */
	private static String getProperty(Properties smssProp, String key, String defaultValue) {
		String value = smssProp.getProperty(key);
		if (value == null || value.isEmpty()) {
			return defaultValue;
		}
		return value;
	}

	/**
	 * Get the authorization header value to send on a request to service now. For
	 * oauth this will pull a new access token when the current one is missing or
	 * expired.
	 *
	 * @return the full authorization header value, ie "Bearer abc" or "Basic abc"
	 */
	public String getAuthorizationHeader() {
		if (this.authType.equals(OAUTH_TYPE)) {
			return this.tokenType + " " + getAccessToken();
		}
		return "Basic " + Base64.getEncoder().encodeToString(
				(this.basicAuthUsername + ":" + this.basicAuthPassword).getBytes(StandardCharsets.UTF_8));
	}

	/**
	 * Get a valid oauth access token. The cached token is reused until it is within
	 * {@link #TOKEN_EXPIRATION_BUFFER_SECONDS} of expiring, at which point a new
	 * one is pulled from the oauth endpoint.
	 *
	 * @return a non expired access token
	 */
	public synchronized String getAccessToken() {
		if (!this.authType.equals(OAUTH_TYPE)) {
			throw new IllegalStateException(
					"Cannot pull an access token when " + AUTH_TYPE_KEY + " is not " + OAUTH_TYPE);
		}
		if (this.accessToken == null || isAccessTokenExpired()) {
			generateAccessToken();
		}
		return this.accessToken;
	}

	/**
	 * Is the current access token expired (or close enough to expiring that we
	 * should not use it)? A token we have never pulled counts as expired.
	 *
	 * @return true when a new access token is needed
	 */
	public synchronized boolean isAccessTokenExpired() {
		if (this.accessToken == null || this.accessTokenExpirationTime < 0) {
			return true;
		}
		return System.currentTimeMillis() >= this.accessTokenExpirationTime;
	}

	/**
	 * Number of seconds left on the current access token. Returns 0 when there is
	 * no usable token.
	 *
	 * @return seconds until the current token expires
	 */
	public synchronized long getSecondsUntilAccessTokenExpires() {
		if (this.accessToken == null || this.accessTokenExpirationTime < 0) {
			return 0;
		}
		long remaining = this.accessTokenExpirationTime - System.currentTimeMillis();
		if (remaining < 0) {
			return 0;
		}
		return remaining / 1000;
	}

	/**
	 * Pull a brand new access token from the oauth endpoint and store the token,
	 * the token type, the refresh token, and when the token expires. If we are
	 * holding a refresh token we use that instead of sending the full credentials
	 * again.
	 */
	private synchronized void generateAccessToken() {
		Map<String, String> headers = new HashMap<>();
		headers.put(HttpHeaders.ACCEPT, "application/json");

		Map<String, String> body = new HashMap<>();
		body.put("client_id", this.oauthClientKey);
		body.put("client_secret", this.oauthSecretKey);
		if (this.refreshToken != null) {
			body.put("grant_type", REFRESH_TOKEN_GRANT);
			body.put("refresh_token", this.refreshToken);
		} else {
			body.put("grant_type", this.grant_type);
			if (this.grant_type.equals(PASSWORD_GRANT)) {
				body.put("username", this.basicAuthUsername);
				body.put("password", this.basicAuthPassword);
			}
		}

		String response = null;
		try {
			response = HttpHelperUtility.postRequestUrlEncodedBody(this.oauthEndpoint, headers, body, null, null, null);
		} catch (Exception e) {
			classLogger.error("Failed to pull an access token from {}", this.oauthEndpoint, e);
			// the refresh token may be what is bad, so clear it and let the next
			// call go back through the full grant
			clearAccessToken();
			throw new IllegalArgumentException("Failed to pull an access token from " + this.oauthEndpoint
					+ ". Detailed message = " + e.getMessage());
		}

		if (response == null || response.trim().isEmpty()) {
			clearAccessToken();
			throw new IllegalArgumentException("Empty response returned from " + this.oauthEndpoint);
		}

		JSONObject responseJson = null;
		try {
			responseJson = new JSONObject(response);
		} catch (Exception e) {
			classLogger.error("Invalid json returned from {}", this.oauthEndpoint, e);
			clearAccessToken();
			throw new IllegalArgumentException("Invalid json returned from " + this.oauthEndpoint);
		}

		if (!responseJson.has(ACCESS_TOKEN_RESPONSE_KEY)) {
			clearAccessToken();
			throw new IllegalArgumentException(
					"No " + ACCESS_TOKEN_RESPONSE_KEY + " returned from " + this.oauthEndpoint);
		}

		this.accessToken = responseJson.getString(ACCESS_TOKEN_RESPONSE_KEY);
		if (this.accessToken == null || (this.accessToken = this.accessToken.trim()).isEmpty()) {
			clearAccessToken();
			throw new IllegalArgumentException(
					"Empty " + ACCESS_TOKEN_RESPONSE_KEY + " returned from " + this.oauthEndpoint);
		}

		if (responseJson.has(TOKEN_TYPE_RESPONSE_KEY)) {
			String responseTokenType = responseJson.optString(TOKEN_TYPE_RESPONSE_KEY);
			if (responseTokenType != null && !(responseTokenType = responseTokenType.trim()).isEmpty()) {
				this.tokenType = responseTokenType;
			}
		}

		if (responseJson.has(REFRESH_TOKEN_RESPONSE_KEY)) {
			String responseRefreshToken = responseJson.optString(REFRESH_TOKEN_RESPONSE_KEY);
			if (responseRefreshToken != null && !(responseRefreshToken = responseRefreshToken.trim()).isEmpty()) {
				this.refreshToken = responseRefreshToken;
			}
		}

		// expires_in comes back in seconds from now
		this.expiresIn = responseJson.optLong(EXPIRES_IN_RESPONSE_KEY, -1);
		if (this.expiresIn > 0) {
			long usableSeconds = this.expiresIn - TOKEN_EXPIRATION_BUFFER_SECONDS;
			if (usableSeconds < 0) {
				usableSeconds = 0;
			}
			this.accessTokenExpirationTime = System.currentTimeMillis() + (usableSeconds * 1000);
		} else {
			// no expiration returned, force a new token on the next call
			this.accessTokenExpirationTime = -1;
		}

		classLogger.info("Pulled a new service now access token from {} that expires in {} seconds", this.oauthEndpoint,
				this.expiresIn);
	}

	/**
	 * Throw away the token we are holding so the next call pulls a new one through
	 * the full grant.
	 */
	public synchronized void clearAccessToken() {
		this.accessToken = null;
		this.refreshToken = null;
		this.expiresIn = -1;
		this.accessTokenExpirationTime = -1;
	}

	/**
	 * Query a service now table with a GET against the table api. Parameters that
	 * map onto a sysparm (query, fields, limit, offset, displayValue, orderBy,
	 * orderByDesc) are pulled out and applied directly. Every other parameter is
	 * treated as a field filter and folded into sysparm_query, so passing
	 * {@code state=1} and {@code priority=1} produces
	 * {@code sysparm_query=state=1^priority=1}.
	 *
	 * @param parameterValues the runtime parameters for this call
	 * @return the list of records under the service now {@code result} key, or the
	 *         raw response when it cannot be parsed
	 */
	@Override
	public Object execute(Map<String, Object> parameterValues) {
		if (parameterValues == null) {
			parameterValues = new HashMap<>();
		}
		Insight executingInsight = (Insight) parameterValues.remove(Constants.INSIGHT);

		// validate all the required keys are set
		if (this.requiredParameters != null && !this.requiredParameters.isEmpty()) {
			Set<String> missingPs = new HashSet<>();
			for (String requiredP : this.requiredParameters) {
				if (!parameterValues.containsKey(requiredP)) {
					missingPs.add(requiredP);
				}
			}
			if (!missingPs.isEmpty()) {
				throw new IllegalArgumentException("Must define required keys = " + missingPs);
			}
		}

		String runTimeTable = getParameterValue(parameterValues, TABLE_PARAM, this.defaultTable);
		if (runTimeTable == null) {
			throw new IllegalArgumentException("Must define the " + TABLE_PARAM + " parameter or the "
					+ DEFAULT_TABLE_KEY + " key in the SMSS to know which service now table to query");
		}

		String url = buildTableQueryUrl(runTimeTable, parameterValues);
		classLogger.info("Querying service now table {} via url {}", runTimeTable, url);

		String response = null;
		try {
			response = executeGetRequest(url);
		} catch (IllegalArgumentException e) {
			// the token may have been revoked on the service now side before it was
			// set to expire, so throw it away and give the request one more shot
			if (this.authType.equals(OAUTH_TYPE) && isUnauthorized(e)) {
				classLogger.warn("Service now returned unauthorized for {}, pulling a new access token and retrying",
						url);
				clearAccessToken();
				response = executeGetRequest(url);
			} else {
				throw e;
			}
		}

		if (response == null || response.trim().isEmpty()) {
			return new ArrayList<>();
		}

		// unwrap the service now envelope so the caller just gets the records
		try {
			Map<String, Object> responseMap = gson.fromJson(response, new TypeToken<Map<String, Object>>() {
			}.getType());
			if (responseMap != null && responseMap.containsKey(RESULT_KEY)) {
				return responseMap.get(RESULT_KEY);
			}
		} catch (Exception e) {
			classLogger.warn("Could not parse the service now response from {}, returning it as is", url, e);
		}
		return response;
	}

	/**
	 * Build the full table api url, including the encoded sysparm query string, for
	 * a single execute call.
	 *
	 * @param runTimeTable    the table being queried
	 * @param parameterValues the runtime parameters for this call
	 * @return the fully constructed request url
	 */
	private String buildTableQueryUrl(String runTimeTable, Map<String, Object> parameterValues) {
		// service now uses ^ to and encoded query clauses together
		StringBuilder encodedQuery = new StringBuilder();
		appendQueryClause(encodedQuery, getParameterValue(parameterValues, QUERY_PARAM, null));

		// anything that is not a recognized sysparm becomes a field=value filter
		for (String paramKey : parameterValues.keySet()) {
			if (RESERVED_PARAMS.contains(paramKey)) {
				continue;
			}
			Object paramValue = parameterValues.get(paramKey);
			if (paramValue == null) {
				continue;
			}
			appendQueryClause(encodedQuery, paramKey + "=" + paramValue);
		}

		String orderBy = getParameterValue(parameterValues, ORDER_BY_PARAM, null);
		if (orderBy != null) {
			appendQueryClause(encodedQuery, "ORDERBY" + orderBy);
		}
		String orderByDesc = getParameterValue(parameterValues, ORDER_BY_DESC_PARAM, null);
		if (orderByDesc != null) {
			appendQueryClause(encodedQuery, "ORDERBYDESC" + orderByDesc);
		}

		Map<String, String> sysparms = new LinkedHashMap<>();
		if (encodedQuery.length() > 0) {
			sysparms.put("sysparm_query", encodedQuery.toString());
		}
		String runTimeFields = getParameterValue(parameterValues, FIELDS_PARAM, this.fields);
		if (runTimeFields != null) {
			sysparms.put("sysparm_fields", runTimeFields);
		}
		String runTimeLimit = getParameterValue(parameterValues, LIMIT_PARAM, this.limit);
		if (runTimeLimit != null) {
			sysparms.put("sysparm_limit", runTimeLimit);
		}
		String runTimeOffset = getParameterValue(parameterValues, OFFSET_PARAM, null);
		if (runTimeOffset != null) {
			sysparms.put("sysparm_offset", runTimeOffset);
		}
		String runTimeDisplayValue = getParameterValue(parameterValues, DISPLAY_VALUE_PARAM, this.displayValue);
		if (runTimeDisplayValue != null) {
			sysparms.put("sysparm_display_value", runTimeDisplayValue);
		}
		if (this.excludeReferenceLink != null) {
			sysparms.put("sysparm_exclude_reference_link", this.excludeReferenceLink);
		}

		StringBuilder url = new StringBuilder(this.endpoint).append(this.tableApiPath)
				.append(encodePathSegments(runTimeTable));
		boolean first = true;
		for (String sysparmKey : sysparms.keySet()) {
			url.append(first ? "?" : "&");
			url.append(sysparmKey).append("=")
					.append(URLEncoder.encode(sysparms.get(sysparmKey), StandardCharsets.UTF_8));
			first = false;
		}
		return url.toString();
	}

	/**
	 * Encode a table value for use in the url path. The value can itself be made up
	 * of multiple path segments, ie a scripted rest api that takes the table as
	 * {@code dap/global_asset}, so each segment is encoded on its own and the
	 * slashes between them are left as separators.
	 *
	 * @param table the table value being placed in the path
	 * @return the encoded path
	 */
	private static String encodePathSegments(String table) {
		StringBuilder encoded = new StringBuilder();
		String[] segments = table.split("/");
		for (String segment : segments) {
			if (segment.isEmpty()) {
				continue;
			}
			if (encoded.length() > 0) {
				encoded.append("/");
			}
			encoded.append(URLEncoder.encode(segment, StandardCharsets.UTF_8));
		}
		return encoded.toString();
	}

	/**
	 * Append a clause onto an encoded query, adding the service now {@code ^} and
	 * separator when the query already has content.
	 *
	 * @param encodedQuery the encoded query being built up
	 * @param clause       the clause to append; ignored when null or blank
	 */
	private static void appendQueryClause(StringBuilder encodedQuery, String clause) {
		if (clause == null || (clause = clause.trim()).isEmpty()) {
			return;
		}
		if (encodedQuery.length() > 0) {
			encodedQuery.append("^");
		}
		encodedQuery.append(clause);
	}

	/**
	 * Execute the GET against service now with the auth and accept headers set.
	 *
	 * @param url the fully constructed request url
	 * @return the raw response body
	 */
	private String executeGetRequest(String url) {
		Map<String, String> headers = new HashMap<>();
		headers.put(HttpHeaders.AUTHORIZATION, getAuthorizationHeader());
		headers.put(HttpHeaders.ACCEPT, "application/json");
		return HttpHelperUtility.getRequest(url, headers, null, null, null);
	}

	/**
	 * Did service now reject the request because of the credentials we sent?
	 * HttpHelperUtility surfaces the status code in the exception message.
	 *
	 * @param e the exception thrown by the request
	 * @return true when the request came back as a 401
	 */
	private static boolean isUnauthorized(Exception e) {
		String message = e.getMessage();
		return message != null && message.contains("returned HTTP 401");
	}

	/**
	 * Pull a parameter as a trimmed string, falling back to a default when it is
	 * missing or blank.
	 *
	 * @param parameterValues the runtime parameters for this call
	 * @param key             the parameter to read
	 * @param defaultValue    value to use when the parameter is not set
	 * @return the parameter value as a string or the default
	 */
	private static String getParameterValue(Map<String, Object> parameterValues, String key, String defaultValue) {
		Object value = parameterValues.get(key);
		if (value == null) {
			return defaultValue;
		}
		String stringValue = value.toString().trim();
		if (stringValue.isEmpty()) {
			return defaultValue;
		}
		return stringValue;
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.SERVICE_NOW.name();
	}

	@Override
	public void close() throws IOException {
		clearAccessToken();
	}

}
