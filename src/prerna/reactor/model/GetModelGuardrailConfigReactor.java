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
package prerna.reactor.model;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

import prerna.auth.User;
import prerna.auth.utils.SecurityEngineUtils;
import prerna.engine.api.IEngine;
import prerna.engine.api.IModelEngine;
import prerna.engine.impl.model.message.AbstractMessage;
import prerna.engine.impl.model.responses.AbstractModelEngineResponse;
import prerna.logging.IgnoreEngineLogging;
import prerna.reactor.AbstractReactor;
import prerna.reactor.interceptor.GenericGuardrailInputReactor;
import prerna.reactor.interceptor.GenericGuardrailOutputReactor;
import prerna.reactor.interceptor.PipelineReactorUtils;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.EngineUtility;
import prerna.util.Utility;

/**
 * Returns a model engine's guardrail pipeline configuration (the file the
 * PIPELINE smss key points at) so the settings UI can load it. Missing or
 * malformed configuration is reported in the response instead of thrown so the
 * UI can offer to create or reset it. The response also carries the model
 * engine methods a pipeline can intercept, so the editor can offer the method
 * and argument names the runtime actually resolves rather than asking for them
 * as free text. Requires edit access - the config exposes the engine ids of the
 * attached guardrail engines.
 */
public class GetModelGuardrailConfigReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(GetModelGuardrailConfigReactor.class);

	private static final String PIPELINES_KEY = "pipelines";
	private static final String INPUT_KEY = "input";
	private static final String OUTPUT_KEY = "output";
	private static final String PARAMS_KEY = "params";
	private static final String GUARDRAIL_ENGINE_ID_KEY = "guardrailEngineId";

	/**
	 * Method names surfaced first so the picker leads with the calls that carry
	 * user content. Everything else follows alphabetically.
	 */
	private static final List<String> METHOD_DISPLAY_ORDER = Arrays.asList("askRoom", "embeddings",
			"multiModalEmbeddings");

	private static final List<Map<String, Object>> INTERCEPTABLE_METHODS = buildInterceptableMethods();

	public GetModelGuardrailConfigReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.ENGINE.getKey() };
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String engineId = this.keyValue.get(this.keysToGet[0]);
		User user = this.insight.getUser();
		if (!SecurityEngineUtils.userCanEditEngine(user, engineId)) {
			throw new IllegalArgumentException(
					"Engine " + engineId + " does not exist or user does not have edit access to it");
		}
		Object[] typeAndSubtype = SecurityEngineUtils.getEngineTypeAndSubtype(engineId);
		if (typeAndSubtype[0] != IEngine.CATALOG_TYPE.MODEL) {
			throw new IllegalArgumentException("Engine " + engineId + " is not a model engine");
		}

		IModelEngine model = Utility.getModel(engineId);
		if (model == null) {
			throw new IllegalArgumentException("Could not load model engine " + engineId);
		}

		String pipelineValue = model.getSmssProp().getProperty(IEngine.PIPELINE);
		boolean configured = pipelineValue != null && !pipelineValue.trim().isEmpty();

		Map<String, Object> response = new HashMap<>();
		response.put("engineId", engineId);
		response.put("configured", configured);
		response.put("pipelineFileName", configured ? pipelineValue.trim() : null);
		response.put("fileExists", false);
		response.put("parseError", null);
		response.put("rawContent", null);

		Map<String, Object> pipelines = new HashMap<>();
		if (configured) {
			String assetsFolder = EngineUtility.getSpecificEngineAssetsFolder(model.getCatalogType(),
					model.getEngineId(), model.getEngineName());
			File pipelineFile = new File((assetsFolder + "/" + pipelineValue.trim()).replace("\\", "/"));
			if (pipelineFile.exists() && pipelineFile.isFile()) {
				response.put("fileExists", true);
				String content = null;
				try {
					content = FileUtils.readFileToString(pipelineFile, "UTF-8");
				} catch (IOException e) {
					throw new IllegalStateException("Unable to read the guardrail configuration: " + e.getMessage(), e);
				}
				try {
					JSONObject root = new JSONObject(content);
					if (root.has(PIPELINES_KEY)) {
						pipelines = root.getJSONObject(PIPELINES_KEY).toMap();
					}
				} catch (JSONException e) {
					response.put("parseError", e.getMessage());
					response.put("rawContent", content);
				}
			}
		}
		response.put(PIPELINES_KEY, pipelines);
		response.put("guardrailEngines", getGuardrailEngineDetails(user, pipelines));

		Map<String, Object> allowedReactorClasses = new HashMap<>();
		allowedReactorClasses.put(INPUT_KEY, Arrays.asList(GenericGuardrailInputReactor.class.getName()));
		allowedReactorClasses.put(OUTPUT_KEY, Arrays.asList(GenericGuardrailOutputReactor.class.getName()));
		response.put("allowedReactorClasses", allowedReactorClasses);

		response.put("interceptableMethods", INTERCEPTABLE_METHODS);
		response.put("resultArgumentName", PipelineReactorUtils.RESULT);

		return new NounMetadata(response, PixelDataType.MAP, PixelOperationType.OPERATION);
	}

	/**
	 * The model engine methods a guardrail pipeline can intercept, each with the
	 * argument names a guardrail maps its parameters from. Methods annotated with
	 * {@link IgnoreEngineLogging} are left out because
	 * {@link prerna.engine.impl.pipeline.PipelineInvocationHandler} invokes the
	 * real engine directly for them, so no pipeline runs.
	 *
	 * Argument names come from {@link Parameter#getName()}, the same lookup the
	 * invocation handler uses to build its argument map. That yields arg0, arg1,
	 * ... unless the interface was compiled with -parameters, in which case the
	 * source names are returned; nameIsFromSource reports which one the running
	 * build produced.
	 *
	 * @return one entry per interceptable method, ordered for display
	 */
	private static List<Map<String, Object>> buildInterceptableMethods() {
		List<Map<String, Object>> interceptable = new ArrayList<>();
		try {
			List<Method> methods = new ArrayList<>();
			for (Method method : IModelEngine.class.getDeclaredMethods()) {
				if (method.isSynthetic() || Modifier.isStatic(method.getModifiers())
						|| method.isAnnotationPresent(IgnoreEngineLogging.class)) {
					continue;
				}
				methods.add(method);
			}
			methods.sort(Comparator.comparingInt((Method method) -> {
				int rank = METHOD_DISPLAY_ORDER.indexOf(method.getName());
				return rank < 0 ? METHOD_DISPLAY_ORDER.size() : rank;
			}).thenComparing(Method::getName));

			for (Method method : methods) {
				Map<String, Object> entry = new LinkedHashMap<>();
				entry.put("name", method.getName());
				entry.put("deprecated", method.isAnnotationPresent(Deprecated.class));
				entry.put("returnType", readableTypeName(method.getGenericReturnType()));
				entry.put("returnsModelResponse",
						AbstractModelEngineResponse.class.isAssignableFrom(method.getReturnType()));

				List<Map<String, Object>> arguments = new ArrayList<>();
				Parameter[] parameters = method.getParameters();
				for (int i = 0; i < parameters.length; i++) {
					Map<String, Object> argument = new LinkedHashMap<>();
					argument.put("name", parameters[i].getName());
					argument.put("position", i);
					argument.put("nameIsFromSource", parameters[i].isNamePresent());
					argument.put("type", readableTypeName(parameters[i].getParameterizedType()));
					argument.put("guardable", isGuardableArgument(parameters[i]));
					arguments.add(argument);
				}
				entry.put("arguments", arguments);
				interceptable.add(entry);
			}
		} catch (RuntimeException e) {
			// the settings UI falls back to free-text entry when this list is empty,
			// so a reflection problem must not take the whole reactor down
			classLogger.error("Unable to reflect the interceptable model engine methods", e);
		}
		return interceptable;
	}

	/**
	 * Whether a guardrail can screen this argument's value. The interceptors reduce
	 * strings, messages, and collections of either to text; every other argument is
	 * call plumbing such as the Insight, the Room, or the provider parameter map,
	 * which a guardrail has no text to read from.
	 *
	 * @param parameter
	 * @return
	 */
	private static boolean isGuardableArgument(Parameter parameter) {
		Type generic = parameter.getParameterizedType();
		if (isScreenableType(generic)) {
			return true;
		}
		if (Collection.class.isAssignableFrom(parameter.getType()) && generic instanceof ParameterizedType) {
			Type[] typeArguments = ((ParameterizedType) generic).getActualTypeArguments();
			return typeArguments.length == 1 && isScreenableType(typeArguments[0]);
		}
		return false;
	}

	/**
	 * Whether {@link prerna.reactor.interceptor.GuardrailValueReader} can reduce
	 * this type to text: a string, a message, or a map serialized to JSON. A
	 * wildcard or type variable is reported as unscreenable even though the
	 * interceptor would still reduce whatever arrives, so the editor understates
	 * rather than promises.
	 *
	 * @param type
	 * @return
	 */
	private static boolean isScreenableType(Type type) {
		if (type instanceof ParameterizedType) {
			return isScreenableType(((ParameterizedType) type).getRawType());
		}
		if (!(type instanceof Class)) {
			return false;
		}
		Class<?> raw = (Class<?>) type;
		return raw == String.class || AbstractMessage.class.isAssignableFrom(raw) || Map.class.isAssignableFrom(raw);
	}

	/**
	 * Drops package qualifiers so the UI prints List&lt;String&gt; rather than
	 * java.util.List&lt;java.lang.String&gt;.
	 *
	 * @param type
	 * @return
	 */
	private static String readableTypeName(Type type) {
		return type.getTypeName().replaceAll("(\\w+\\.)+", "");
	}

	/**
	 * Resolve name/type details for every guardrail engine referenced in the config
	 * so the UI can label them without extra pixel calls. Referenced engines that
	 * no longer exist are reported with exists=false.
	 *
	 * @param user
	 * @param pipelines
	 * @return
	 */
	private static Map<String, Object> getGuardrailEngineDetails(User user, Map<String, Object> pipelines) {
		Set<String> guardrailEngineIds = collectGuardrailEngineIds(pipelines);
		Map<String, Object> details = new HashMap<>();
		if (guardrailEngineIds.isEmpty()) {
			return details;
		}
		Map<Object, Object> aliases = SecurityEngineUtils.getEngineAliasForIds(guardrailEngineIds);
		for (String guardrailEngineId : guardrailEngineIds) {
			Map<String, Object> info = new HashMap<>();
			Object alias = aliases.get(guardrailEngineId);
			info.put("name", alias == null ? null : alias.toString());
			boolean exists = true;
			String type = null;
			String subtype = null;
			try {
				Object[] typeAndSubtype = SecurityEngineUtils.getEngineTypeAndSubtype(guardrailEngineId);
				type = String.valueOf(typeAndSubtype[0]);
				subtype = typeAndSubtype[1] == null ? null : typeAndSubtype[1].toString();
			} catch (IllegalArgumentException e) {
				exists = false;
			}
			info.put("exists", exists);
			info.put("type", type);
			info.put("subtype", subtype);
			info.put("userCanView", exists && SecurityEngineUtils.userCanViewEngine(user, guardrailEngineId));
			details.put(guardrailEngineId, info);
		}
		return details;
	}

	/**
	 * Walk the parsed pipelines and pull out every guardrailEngineId. The walk is
	 * defensive - malformed nodes are skipped, not thrown, since this reactor
	 * reports config problems instead of failing on them.
	 *
	 * @param pipelines
	 * @return
	 */
	private static Set<String> collectGuardrailEngineIds(Map<String, Object> pipelines) {
		Set<String> guardrailEngineIds = new LinkedHashSet<>();
		for (Object pipeline : pipelines.values()) {
			if (!(pipeline instanceof Map)) {
				continue;
			}
			Map<?, ?> pipelineMap = (Map<?, ?>) pipeline;
			collectFromSlot(pipelineMap.get(INPUT_KEY), guardrailEngineIds);
			collectFromSlot(pipelineMap.get(OUTPUT_KEY), guardrailEngineIds);
		}
		return guardrailEngineIds;
	}

	private static void collectFromSlot(Object slot, Set<String> guardrailEngineIds) {
		if (!(slot instanceof List)) {
			return;
		}
		for (Object entry : (List<?>) slot) {
			if (!(entry instanceof Map)) {
				continue;
			}
			Object params = ((Map<?, ?>) entry).get(PARAMS_KEY);
			if (!(params instanceof Map)) {
				continue;
			}
			Object guardrailEngineId = ((Map<?, ?>) params).get(GUARDRAIL_ENGINE_ID_KEY);
			if (guardrailEngineId instanceof String && !((String) guardrailEngineId).trim().isEmpty()) {
				guardrailEngineIds.add((String) guardrailEngineId);
			}
		}
	}

	@Override
	public String getReactorDescription() {
		return "Returns the guardrail pipeline configuration (pipeline.json contents) for a model engine, along with name and type details for every referenced guardrail engine and the list of model engine methods a pipeline can intercept. Requires edit access to the engine.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.ENGINE.getKey())) {
			return "The id of the model engine";
		}
		return super.getDescriptionForKey(key);
	}
}
