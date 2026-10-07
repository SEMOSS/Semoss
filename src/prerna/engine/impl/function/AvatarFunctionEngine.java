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
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.hc.core5.http.ContentType;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.GsonBuilder;
import com.google.gson.reflect.TypeToken;

import prerna.engine.api.FunctionTypeEnum;
import prerna.security.HttpHelperUtility;
import prerna.util.Constants;
import prerna.util.Utility;

/**
 * Brokers a real-time avatar session with an avatar provider. The engine never
 * carries media: it creates the session, hands the caller the provider's
 * session details so the browser can connect to the stream directly, and then
 * relays speak / interrupt / close commands. The provider API key stays in the
 * SMSS and is never returned.
 *
 * <p>
 * Only HeyGen is implemented. The provider is picked by AVATAR_PROVIDER so
 * another one can be added without a new engine type.
 */
public class AvatarFunctionEngine extends AbstractFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AvatarFunctionEngine.class);

	private static String API_KEY_KEY = "API_KEY";
	private static String API_BASE_KEY = "API_BASE";
	private static String AVATAR_PROVIDER_KEY = "AVATAR_PROVIDER";
	private static String DEFAULT_AVATAR_ID_KEY = "DEFAULT_AVATAR_ID";
	private static String DEFAULT_VOICE_ID_KEY = "DEFAULT_VOICE_ID";
	private static String DEFAULT_QUALITY_KEY = "DEFAULT_QUALITY";

	private static String PROVIDER_HEYGEN = "HEYGEN";
	private static String DEFAULT_API_BASE = "https://api.heygen.com";

	// heygen authenticates on this header
	private static String API_KEY_HEADER = "x-api-key";

	// provider routes. unverified against the current heygen docs - confirm these
	// before relying on the engine. kept in one place so a fix is one edit
	private static String CREATE_SESSION_PATH = "/v1/streaming.new";
	private static String START_SESSION_PATH = "/v1/streaming.start";
	private static String SPEAK_PATH = "/v1/streaming.task";
	private static String INTERRUPT_PATH = "/v1/streaming.interrupt";
	private static String CLOSE_SESSION_PATH = "/v1/streaming.stop";
	private static String LIST_AVATARS_PATH = "/v1/streaming/avatar.list";
	private static String LIST_VOICES_PATH = "/v2/voices";

	// parameters execute understands
	public static String ACTION_PARAM = "action";
	public static String SESSION_ID_PARAM = "sessionId";
	public static String TEXT_PARAM = "text";
	public static String AVATAR_ID_PARAM = "avatarId";
	public static String VOICE_ID_PARAM = "voiceId";
	public static String QUALITY_PARAM = "quality";
	public static String TASK_TYPE_PARAM = "taskType";

	public static String CREATE_SESSION_ACTION = "create_session";
	public static String SPEAK_ACTION = "speak";
	public static String INTERRUPT_ACTION = "interrupt";
	public static String CLOSE_SESSION_ACTION = "close_session";
	public static String LIST_AVATARS_ACTION = "list_avatars";
	public static String LIST_VOICES_ACTION = "list_voices";

	private static List<String> ACTIONS = Arrays.asList(CREATE_SESSION_ACTION, SPEAK_ACTION, INTERRUPT_ACTION,
			CLOSE_SESSION_ACTION, LIST_AVATARS_ACTION, LIST_VOICES_ACTION);

	private String apiKey = null;
	private String apiBase = DEFAULT_API_BASE;
	private String provider = PROVIDER_HEYGEN;
	private String defaultAvatarId = null;
	private String defaultVoiceId = null;
	private String defaultQuality = "medium";

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.provider = StringUtils.defaultIfEmpty(smssProp.getProperty(AVATAR_PROVIDER_KEY), this.provider)
				.trim().toUpperCase();
		if (!PROVIDER_HEYGEN.equals(this.provider)) {
			throw new IllegalArgumentException(
					"Unsupported " + AVATAR_PROVIDER_KEY + " '" + this.provider + "'. Supported: " + PROVIDER_HEYGEN);
		}

		this.apiKey = smssProp.getProperty(API_KEY_KEY);
		if (StringUtils.isEmpty(this.apiKey)) {
			throw new IllegalArgumentException("Must have key " + API_KEY_KEY + " in SMSS");
		}

		// the UI writes a blank line for every optional field left empty
		this.apiBase = StringUtils.defaultIfEmpty(smssProp.getProperty(API_BASE_KEY), this.apiBase);
		while (this.apiBase.endsWith("/")) {
			this.apiBase = this.apiBase.substring(0, this.apiBase.length() - 1);
		}
		Utility.checkIfValidDomain(this.apiBase);

		this.defaultAvatarId = StringUtils.defaultIfEmpty(smssProp.getProperty(DEFAULT_AVATAR_ID_KEY),
				this.defaultAvatarId);
		this.defaultVoiceId = StringUtils.defaultIfEmpty(smssProp.getProperty(DEFAULT_VOICE_ID_KEY),
				this.defaultVoiceId);
		this.defaultQuality = StringUtils.defaultIfEmpty(smssProp.getProperty(DEFAULT_QUALITY_KEY),
				this.defaultQuality);

		setDefaultFunctionMetadata();
	}

	/**
	 * Fill in the function metadata the SMSS did not define. A value set in the
	 * SMSS is never overwritten.
	 */
	private void setDefaultFunctionMetadata() {
		if (this.functionDescription == null || this.functionDescription.isEmpty()) {
			this.functionDescription = """
					Control a real-time AI avatar. Use create_session to start an avatar and get the stream \
					details the browser connects to, speak to make the avatar say text, interrupt to cut off \
					current speech, and close_session when done so the session stops billing. \
					list_avatars and list_voices show what can be used.\
					""";
		}

		if (this.parameters == null || this.parameters.isEmpty()) {
			List<FunctionParameter> defaultParameters = new ArrayList<>();
			defaultParameters.add(new FunctionParameter(ACTION_PARAM, "string",
					"What to do. One of " + String.join(", ", ACTIONS) + "."));
			defaultParameters.add(new FunctionParameter(SESSION_ID_PARAM, "string",
					"The session id returned by create_session. Required for speak, interrupt and close_session."));
			defaultParameters.add(new FunctionParameter(TEXT_PARAM, "string",
					"What the avatar should say. Required for speak."));
			defaultParameters.add(new FunctionParameter(AVATAR_ID_PARAM, "string",
					"Optional avatar to use for create_session." + defaultText(this.defaultAvatarId)));
			defaultParameters.add(new FunctionParameter(VOICE_ID_PARAM, "string",
					"Optional voice to use for create_session." + defaultText(this.defaultVoiceId)));
			defaultParameters.add(new FunctionParameter(QUALITY_PARAM, "string",
					"Optional stream quality for create_session: low, medium or high."
							+ defaultText(this.defaultQuality)));
			defaultParameters.add(new FunctionParameter(TASK_TYPE_PARAM, "string",
					"Optional for speak. repeat makes the avatar say the text exactly; talk lets the provider "
							+ "answer it. Defaults to repeat."));
			this.parameters = defaultParameters;
		}

		if (this.requiredParameters == null || this.requiredParameters.isEmpty()) {
			this.requiredParameters = new ArrayList<>(Arrays.asList(ACTION_PARAM));
		}
	}

	@Override
	public Object execute(Map<String, Object> parameterValues) {
		if (parameterValues == null) {
			parameterValues = new HashMap<>();
		}
		// the insight rides along on the parameter map but is not an avatar parameter
		parameterValues.remove(Constants.INSIGHT);

		validateRequiredParameters(parameterValues);

		String action = getParameterValue(parameterValues, ACTION_PARAM, "").toLowerCase();
		if (CREATE_SESSION_ACTION.equals(action)) {
			return createSession(parameterValues);
		} else if (SPEAK_ACTION.equals(action)) {
			return speak(parameterValues);
		} else if (INTERRUPT_ACTION.equals(action)) {
			return sessionCommand(INTERRUPT_PATH, requireSessionId(parameterValues));
		} else if (CLOSE_SESSION_ACTION.equals(action)) {
			return sessionCommand(CLOSE_SESSION_PATH, requireSessionId(parameterValues));
		} else if (LIST_AVATARS_ACTION.equals(action)) {
			return parse(sendRequest("GET", LIST_AVATARS_PATH, null));
		} else if (LIST_VOICES_ACTION.equals(action)) {
			return parse(sendRequest("GET", LIST_VOICES_PATH, null));
		}
		throw new IllegalArgumentException(
				"Unknown " + ACTION_PARAM + " '" + action + "'. Must be one of " + String.join(", ", ACTIONS));
	}

	private Object createSession(Map<String, Object> parameterValues) {
		Map<String, Object> body = new HashMap<>();
		body.put("quality", getParameterValue(parameterValues, QUALITY_PARAM, this.defaultQuality));
		String avatarId = getParameterValue(parameterValues, AVATAR_ID_PARAM, this.defaultAvatarId);
		if (avatarId != null) {
			body.put("avatar_id", avatarId);
		}
		String voiceId = getParameterValue(parameterValues, VOICE_ID_PARAM, this.defaultVoiceId);
		if (voiceId != null) {
			Map<String, Object> voice = new HashMap<>();
			voice.put("voice_id", voiceId);
			body.put("voice", voice);
		}

		Object created = parse(sendRequest("POST", CREATE_SESSION_PATH, body));

		// heygen needs an explicit start after new before the stream carries media
		String sessionId = extractSessionId(created);
		if (sessionId != null) {
			Map<String, Object> startBody = new HashMap<>();
			startBody.put("session_id", sessionId);
			sendRequest("POST", START_SESSION_PATH, startBody);
		}
		return created;
	}

	private Object speak(Map<String, Object> parameterValues) {
		String sessionId = requireSessionId(parameterValues);
		String text = getParameterValue(parameterValues, TEXT_PARAM, null);
		if (text == null) {
			throw new IllegalArgumentException("Must define the " + TEXT_PARAM + " parameter for " + SPEAK_ACTION);
		}
		String taskType = getParameterValue(parameterValues, TASK_TYPE_PARAM, "repeat").toLowerCase();
		if (!taskType.equals("repeat") && !taskType.equals("talk")) {
			throw new IllegalArgumentException("Invalid " + TASK_TYPE_PARAM + " '" + taskType + "'. Use repeat or talk");
		}

		Map<String, Object> body = new HashMap<>();
		body.put("session_id", sessionId);
		body.put("text", text);
		body.put("task_type", taskType);
		return parse(sendRequest("POST", SPEAK_PATH, body));
	}

	private Object sessionCommand(String path, String sessionId) {
		Map<String, Object> body = new HashMap<>();
		body.put("session_id", sessionId);
		return parse(sendRequest("POST", path, body));
	}

	private String requireSessionId(Map<String, Object> parameterValues) {
		String sessionId = getParameterValue(parameterValues, SESSION_ID_PARAM, null);
		if (sessionId == null) {
			throw new IllegalArgumentException("Must define the " + SESSION_ID_PARAM + " parameter");
		}
		return sessionId;
	}

	/**
	 * Heygen nests the session id under data on most responses.
	 */
	@SuppressWarnings("unchecked")
	private String extractSessionId(Object created) {
		if (!(created instanceof Map)) {
			return null;
		}
		Object data = ((Map<String, Object>) created).get("data");
		if (data instanceof Map) {
			Object id = ((Map<String, Object>) data).get("session_id");
			return id == null ? null : id.toString();
		}
		return null;
	}

	/**
	 * Send one provider request. Protected so tests can replace the network call.
	 *
	 * @param method GET or POST
	 * @param path   route relative to the API base
	 * @param body   JSON body for a POST, or null
	 * @return the response body, or null when the provider returned none
	 */
	protected String sendRequest(String method, String path, Map<String, Object> body) {
		String url = this.apiBase + path;
		Map<String, String> headers = new HashMap<>();
		headers.put(API_KEY_HEADER, this.apiKey);
		headers.put("Accept", "application/json");
		classLogger.info("Avatar {} {}", method, path);
		if ("GET".equals(method)) {
			return HttpHelperUtility.getRequest(url, headers, null, null, null);
		}
		String json = new GsonBuilder().disableHtmlEscaping().create().toJson(body == null ? new HashMap<>() : body);
		return HttpHelperUtility.postRequestStringBody(url, headers, json, ContentType.APPLICATION_JSON, null, null,
				null);
	}

	/**
	 * Parse a provider response into a map, or hand back the raw text when it is
	 * not a JSON object.
	 */
	private Object parse(String response) {
		if (response == null || response.trim().isEmpty()) {
			return new HashMap<String, Object>();
		}
		try {
			Map<String, Object> parsed = this.gson.fromJson(response, new TypeToken<Map<String, Object>>() {
			}.getType());
			return parsed == null ? response : parsed;
		} catch (Exception e) {
			return response;
		}
	}

	@Override
	public void close() throws IOException {
		// no persistent connection to release. sessions are closed by close_session
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.AVATAR.getFunctionName();
	}

}
