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
 * Brokers a real-time avatar session with a self-hosted avatar server.
 *
 * <p>
 * Media never passes through this engine. The browser sends its WebRTC offer to
 * the engine (action connect), the engine relays it to the avatar server and
 * returns the answer, and the video then flows directly between the browser and
 * the avatar server. Keeping the signalling and control calls on the server side
 * means the avatar server's HTTP API, which has no authentication of its own,
 * never has to be exposed to browsers. Only its media ports do.
 *
 * <p>
 * Only LiveTalking (https://github.com/lipku/LiveTalking, Apache-2.0) is
 * implemented, chosen by AVATAR_PROVIDER. Speech is always sent as finished
 * text in echo mode, so the avatar server never calls an LLM or any other
 * outside service of its own: the language model is a SEMOSS model engine.
 */
public class AvatarFunctionEngine extends AbstractFunctionEngine {

	private static final Logger classLogger = LogManager.getLogger(AvatarFunctionEngine.class);

	private static String SERVER_URL_KEY = "SERVER_URL";
	private static String AVATAR_PROVIDER_KEY = "AVATAR_PROVIDER";
	private static String DEFAULT_AVATAR_ID_KEY = "DEFAULT_AVATAR_ID";
	private static String DEFAULT_AVATAR_MODEL_KEY = "DEFAULT_AVATAR_MODEL";
	private static String HEADERS_KEY = "HEADERS";

	private static String PROVIDER_LIVETALKING = "LIVETALKING";

	// livetalking routes, from docs/api.md and docs/avatar_api.md in its repo
	private static String OFFER_PATH = "/offer";
	private static String SPEAK_PATH = "/human";
	private static String INTERRUPT_PATH = "/interrupt_talk";
	private static String IS_SPEAKING_PATH = "/is_speaking";
	private static String AVATAR_TASK_PATH = "/api/avatar/task";

	// parameters execute understands
	public static String ACTION_PARAM = "action";
	public static String SDP_PARAM = "sdp";
	public static String SDP_TYPE_PARAM = "type";
	public static String AVATAR_ID_PARAM = "avatarId";
	public static String SESSION_ID_PARAM = "sessionId";
	public static String TEXT_PARAM = "text";
	public static String INTERRUPT_FIRST_PARAM = "interrupt";
	public static String MODEL_PARAM = "model";
	public static String VIDEO_PATH_PARAM = "videoPath";
	public static String TASK_ID_PARAM = "taskId";

	public static String CONNECT_ACTION = "connect";
	public static String SPEAK_ACTION = "speak";
	public static String INTERRUPT_ACTION = "interrupt";
	public static String IS_SPEAKING_ACTION = "is_speaking";
	public static String CREATE_AVATAR_ACTION = "create_avatar";
	public static String AVATAR_STATUS_ACTION = "avatar_status";

	private static List<String> ACTIONS = Arrays.asList(CONNECT_ACTION, SPEAK_ACTION, INTERRUPT_ACTION,
			IS_SPEAKING_ACTION, CREATE_AVATAR_ACTION, AVATAR_STATUS_ACTION);

	// models the avatar server can build an avatar for. wav2lip is deliberately
	// absent: its weights are licensed for research and non-commercial use only
	private static List<String> AVATAR_MODELS = Arrays.asList("musetalk");

	private String serverUrl = null;
	private String provider = PROVIDER_LIVETALKING;
	private String defaultAvatarId = null;
	private String defaultAvatarModel = "musetalk";
	private Map<String, String> headers = new HashMap<>();

	@Override
	public void open(Properties smssProp) throws Exception {
		super.open(smssProp);

		this.provider = StringUtils.defaultIfEmpty(smssProp.getProperty(AVATAR_PROVIDER_KEY), this.provider).trim()
				.toUpperCase();
		if (!PROVIDER_LIVETALKING.equals(this.provider)) {
			throw new IllegalArgumentException("Unsupported " + AVATAR_PROVIDER_KEY + " '" + this.provider
					+ "'. Supported: " + PROVIDER_LIVETALKING);
		}

		this.serverUrl = smssProp.getProperty(SERVER_URL_KEY);
		if (StringUtils.isBlank(this.serverUrl)) {
			throw new IllegalArgumentException("Must have key " + SERVER_URL_KEY + " in SMSS");
		}
		this.serverUrl = this.serverUrl.trim();
		while (this.serverUrl.endsWith("/")) {
			this.serverUrl = this.serverUrl.substring(0, this.serverUrl.length() - 1);
		}
		Utility.checkIfValidDomain(this.serverUrl);

		// the UI writes a blank line for every optional field left empty
		this.defaultAvatarId = StringUtils.defaultIfEmpty(smssProp.getProperty(DEFAULT_AVATAR_ID_KEY),
				this.defaultAvatarId);
		this.defaultAvatarModel = StringUtils.defaultIfEmpty(smssProp.getProperty(DEFAULT_AVATAR_MODEL_KEY),
				this.defaultAvatarModel).trim().toLowerCase();
		if (!AVATAR_MODELS.contains(this.defaultAvatarModel)) {
			throw new IllegalArgumentException("Unsupported " + DEFAULT_AVATAR_MODEL_KEY + " '"
					+ this.defaultAvatarModel + "'. Supported: " + String.join(", ", AVATAR_MODELS));
		}

		String headersStr = smssProp.getProperty(HEADERS_KEY);
		if (StringUtils.isNotBlank(headersStr)) {
			Map<String, String> parsed = new GsonBuilder().disableHtmlEscaping().create().fromJson(headersStr,
					new TypeToken<Map<String, String>>() {
					}.getType());
			if (parsed != null) {
				this.headers.putAll(parsed);
			}
		}

		setDefaultFunctionMetadata();
	}

	/**
	 * Fill in the function metadata the SMSS did not define. A value set in the
	 * SMSS is never overwritten.
	 */
	private void setDefaultFunctionMetadata() {
		if (this.functionDescription == null || this.functionDescription.isEmpty()) {
			this.functionDescription = """
					Control a real-time AI avatar. A browser calls connect with its WebRTC offer to start a \
					session and receives an answer and a session id. speak makes the avatar say text, \
					interrupt cuts off current speech, and is_speaking reports whether it is talking. \
					create_avatar builds a new avatar from a video and avatar_status reports its progress. \
					A session ends when the browser closes its connection.\
					""";
		}

		if (this.parameters == null || this.parameters.isEmpty()) {
			List<FunctionParameter> defaultParameters = new ArrayList<>();
			defaultParameters.add(new FunctionParameter(ACTION_PARAM, "string",
					"What to do. One of " + String.join(", ", ACTIONS) + "."));
			defaultParameters.add(new FunctionParameter(SDP_PARAM, "string",
					"The browser's WebRTC offer SDP. Required for connect."));
			defaultParameters.add(new FunctionParameter(SDP_TYPE_PARAM, "string",
					"The offer type, always offer. Optional for connect. Defaults to offer."));
			defaultParameters.add(new FunctionParameter(AVATAR_ID_PARAM, "string",
					"Optional avatar to use for connect." + defaultText(this.defaultAvatarId)));
			defaultParameters.add(new FunctionParameter(SESSION_ID_PARAM, "string",
					"The session id returned by connect. Required for speak, interrupt and is_speaking."));
			defaultParameters.add(new FunctionParameter(TEXT_PARAM, "string",
					"What the avatar should say. Required for speak."));
			defaultParameters.add(new FunctionParameter(INTERRUPT_FIRST_PARAM, "boolean",
					"Optional for speak. Cut off current speech before saying this. Defaults to false."));
			defaultParameters.add(new FunctionParameter(MODEL_PARAM, "string",
					"Optional for create_avatar. Lip-sync model to build the avatar for. Only musetalk is "
							+ "supported." + defaultText(this.defaultAvatarModel)));
			defaultParameters.add(new FunctionParameter(VIDEO_PATH_PARAM, "string",
					"Path on the avatar server to the source video. Required for create_avatar."));
			defaultParameters.add(new FunctionParameter(TASK_ID_PARAM, "string",
					"The task id returned by create_avatar. Required for avatar_status."));
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
		if (CONNECT_ACTION.equals(action)) {
			return connect(parameterValues);
		} else if (SPEAK_ACTION.equals(action)) {
			return speak(parameterValues);
		} else if (INTERRUPT_ACTION.equals(action)) {
			return sessionCommand(INTERRUPT_PATH, requireSessionId(parameterValues));
		} else if (IS_SPEAKING_ACTION.equals(action)) {
			return sessionCommand(IS_SPEAKING_PATH, requireSessionId(parameterValues));
		} else if (CREATE_AVATAR_ACTION.equals(action)) {
			return createAvatar(parameterValues);
		} else if (AVATAR_STATUS_ACTION.equals(action)) {
			String taskId = getParameterValue(parameterValues, TASK_ID_PARAM, null);
			if (taskId == null) {
				throw new IllegalArgumentException("Must define the " + TASK_ID_PARAM + " parameter");
			}
			return unwrap(parse(sendRequest("GET", AVATAR_TASK_PATH + "/" + encodeSegment(taskId), null)));
		}
		throw new IllegalArgumentException(
				"Unknown " + ACTION_PARAM + " '" + action + "'. Must be one of " + String.join(", ", ACTIONS));
	}

	/**
	 * Relay the browser's WebRTC offer and hand back the answer plus the session id.
	 * The video then flows between the browser and the avatar server directly.
	 */
	private Object connect(Map<String, Object> parameterValues) {
		String sdp = getParameterValue(parameterValues, SDP_PARAM, null);
		if (sdp == null) {
			throw new IllegalArgumentException("Must define the " + SDP_PARAM + " parameter for " + CONNECT_ACTION);
		}
		Map<String, Object> body = new HashMap<>();
		body.put("sdp", sdp);
		body.put("type", getParameterValue(parameterValues, SDP_TYPE_PARAM, "offer"));
		String avatarId = getParameterValue(parameterValues, AVATAR_ID_PARAM, this.defaultAvatarId);
		if (avatarId != null) {
			body.put("avatar", avatarId);
		}
		// the offer response is the raw answer, not the code/msg/data envelope
		return parse(sendRequest("POST", OFFER_PATH, body));
	}

	private Object speak(Map<String, Object> parameterValues) {
		String sessionId = requireSessionId(parameterValues);
		String text = getParameterValue(parameterValues, TEXT_PARAM, null);
		if (text == null) {
			throw new IllegalArgumentException("Must define the " + TEXT_PARAM + " parameter for " + SPEAK_ACTION);
		}
		Map<String, Object> body = new HashMap<>();
		body.put("sessionid", sessionId);
		body.put("text", text);
		// echo only. chat would hand the text to an LLM the avatar server calls itself
		body.put("type", "echo");
		body.put("interrupt", getBooleanParameterValue(parameterValues, INTERRUPT_FIRST_PARAM, false));
		return unwrap(parse(sendRequest("POST", SPEAK_PATH, body)));
	}

	private Object sessionCommand(String path, String sessionId) {
		Map<String, Object> body = new HashMap<>();
		body.put("sessionid", sessionId);
		return unwrap(parse(sendRequest("POST", path, body)));
	}

	private Object createAvatar(Map<String, Object> parameterValues) {
		String avatarId = getParameterValue(parameterValues, AVATAR_ID_PARAM, null);
		if (avatarId == null) {
			throw new IllegalArgumentException(
					"Must define the " + AVATAR_ID_PARAM + " parameter for " + CREATE_AVATAR_ACTION);
		}
		String videoPath = getParameterValue(parameterValues, VIDEO_PATH_PARAM, null);
		if (videoPath == null) {
			throw new IllegalArgumentException(
					"Must define the " + VIDEO_PATH_PARAM + " parameter for " + CREATE_AVATAR_ACTION);
		}
		String model = getParameterValue(parameterValues, MODEL_PARAM, this.defaultAvatarModel).toLowerCase();
		if (!AVATAR_MODELS.contains(model)) {
			throw new IllegalArgumentException(
					"Unsupported " + MODEL_PARAM + " '" + model + "'. Supported: " + String.join(", ", AVATAR_MODELS));
		}
		Map<String, Object> body = new HashMap<>();
		body.put("model", model);
		body.put("avatar_id", avatarId);
		body.put("video_path", videoPath);
		return unwrap(parse(sendRequest("POST", AVATAR_TASK_PATH, body)));
	}

	private String requireSessionId(Map<String, Object> parameterValues) {
		String sessionId = getParameterValue(parameterValues, SESSION_ID_PARAM, null);
		if (sessionId == null) {
			throw new IllegalArgumentException("Must define the " + SESSION_ID_PARAM + " parameter");
		}
		return sessionId;
	}

	/**
	 * Keep a caller-supplied id from changing the route it is appended to.
	 */
	private static String encodeSegment(String segment) {
		return java.net.URLEncoder.encode(segment, java.nio.charset.StandardCharsets.UTF_8);
	}

	/**
	 * The avatar server wraps most answers as {code, msg, data}, where a non-zero
	 * code is an error even on an HTTP 200.
	 */
	@SuppressWarnings("unchecked")
	private Object unwrap(Object response) {
		if (response instanceof Map) {
			Map<String, Object> map = (Map<String, Object>) response;
			Object code = map.get("code");
			if (code instanceof Number && ((Number) code).intValue() != 0) {
				throw new IllegalArgumentException("Avatar server error: " + map.get("msg"));
			}
			if (map.containsKey("data")) {
				return map.get("data") == null ? map : map.get("data");
			}
		}
		return response;
	}

	/**
	 * Send one request to the avatar server. Protected so tests can replace the
	 * network call.
	 *
	 * @param method GET or POST
	 * @param path   route relative to the server URL
	 * @param body   JSON body for a POST, or null
	 * @return the response body, or null when the server returned none
	 */
	protected String sendRequest(String method, String path, Map<String, Object> body) {
		String url = this.serverUrl + path;
		Map<String, String> requestHeaders = new HashMap<>(this.headers);
		requestHeaders.put("Accept", "application/json");
		classLogger.info("Avatar {} {}", method, path);
		if ("GET".equals(method)) {
			return HttpHelperUtility.getRequest(url, requestHeaders, null, null, null);
		}
		String json = new GsonBuilder().disableHtmlEscaping().create().toJson(body == null ? new HashMap<>() : body);
		return HttpHelperUtility.postRequestStringBody(url, requestHeaders, json, ContentType.APPLICATION_JSON, null,
				null, null);
	}

	/**
	 * Parse a response into a map, or hand back the raw text when it is not a JSON
	 * object.
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
		// no persistent connection to release. a session ends when the browser closes
		// its peer connection
	}

	@Override
	public String getCatalogSubType(Properties smssProp) {
		return FunctionTypeEnum.AVATAR.getFunctionName();
	}

}
