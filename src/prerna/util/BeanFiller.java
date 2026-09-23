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
package prerna.util;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.beanutils.PropertyUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

import io.burt.jmespath.Expression;
import io.burt.jmespath.JmesPath;
import io.burt.jmespath.jackson.JacksonRuntime;

public class BeanFiller {

	protected static final Logger classLogger = LogManager.getLogger(BeanFiller.class);

	/**
	 * Shared JSON parser used for all BeanFiller operations.
	 */
	private static ObjectMapper mapper = new ObjectMapper();
	/**
	 * Shared JMESPath runtime for evaluating user-defined JSON extraction patterns.
	 */
	private static JmesPath<JsonNode> jmespath = new JacksonRuntime();

	private BeanFiller() {

	}

	/**
	 * Evaluate a JMESPath expression against a JSON payload.
	 * 
	 * @param json        source JSON payload
	 * @param jsonPattern JMESPath expression used to select values
	 * @return matched JsonNode (or null if evaluation fails)
	 */
	public static JsonNode getJmesResult(String json, String jsonPattern) {
		try {
			Expression<JsonNode> expression = jmespath.compile(jsonPattern);
			JsonNode input = mapper.readTree(json);
			JsonNode result = expression.search(input);

			return result;
		} catch (Exception ex) {
			classLogger.error("Failed to evaluate JMESPath pattern '{}' for BeanFiller JSON extraction", jsonPattern,
					ex);
		}
		return null;
	}

	/**
	 * Fill a bean (or list of beans) from JSON using a dynamic mapping contract.
	 * <p>
	 * Behavior depends on the evaluated JMESPath result:
	 * <ul>
	 * <li>If the result is an array of objects, each object is mapped to a new bean
	 * instance and a {@code List<Object>} is returned.</li>
	 * <li>Otherwise, the result is treated as a single object/array and mapped into
	 * the provided bean instance.</li>
	 * </ul>
	 * 
	 * @param json        source JSON payload
	 * @param jsonPattern JMESPath expression used to extract data
	 * @param beanProps   runtime bean property mapping configuration
	 * @param bean        target bean instance (also used as the type template when
	 *                    creating list entries)
	 * @return mapped bean instance or {@code List<Object>} depending on result
	 *         shape
	 */
	public static Object fillFromJson(String json, String jsonPattern, String[] beanProps, Object bean) {
		// make the class
		Object retObject = null;
		try {
			Expression<JsonNode> expression = jmespath.compile(jsonPattern);

			// AccessToken tok = mapper.readValue(json, AccessToken.class);
			JsonNode input = mapper.readTree(json);
			JsonNode result = expression.search(input);

			if ((result instanceof ArrayNode) && result.get(0) instanceof ObjectNode) {
				// array-of-object result: create one bean per object entry
				List<Object> retList = new ArrayList<>();
				for (int resIndex = 0; resIndex < result.size(); resIndex++) {
					Object newBean = bean.getClass().getDeclaredConstructor().newInstance();
					Object newObject = null;
					newObject = fillSingleObjectFromMap(result.get(resIndex), beanProps, newBean);
					retList.add(newObject);
				}
				retObject = retList;
			} else {
				retObject = fillSingleObject(result, beanProps, bean);
			}
		} catch (Exception ex) {
			String beanType = bean == null ? "null" : bean.getClass().getName();
			classLogger.error("Failed to fill bean type '{}' from JSON using JMESPath pattern '{}'", beanType,
					jsonPattern, ex);
		}
		return retObject;
	}

	/**
	 * Determine whether the first element of a result node is itself an array.
	 * <p>
	 * This is a lightweight helper used by callers that need to understand whether
	 * the selected JMES result is nested.
	 * 
	 * @param node result node to inspect
	 * @return true when {@code node[0]} is an {@link ArrayNode}, otherwise false
	 */
	public static boolean isJsonArray(JsonNode node) {
		boolean array = false;
		// get the first element
		// if it is an array then proceed with that..
		if (node.size() > 0) {
			JsonNode firstNode = node.get(0);
			if (firstNode instanceof ArrayNode) {
				array = true;
			}
		}
		return array;
	}

	/**
	 * Fill a bean from an ordered result set.
	 * <p>
	 * Each result value is matched by index to {@code beanProps[inputIndex]}. If a
	 * property name starts with {@code add_}, the value is appended to a list-like
	 * bean property. If extra values exist beyond configured properties, they are
	 * written to the bean {@code extra} property.
	 * 
	 * @param result    ordered values from JMES result
	 * @param beanProps bean property names in positional order
	 * @param bean      target bean instance
	 * @return populated bean
	 */
	public static Object fillSingleObject(JsonNode result, String[] beanProps, Object bean) {
		try {
			for (int inputIndex = 0; result != null && inputIndex < result.size(); inputIndex++) {
				String thisInput = result.get(inputIndex).asText();
				if (beanProps.length > inputIndex) {
					String beanProp = beanProps[inputIndex];
					if (beanProp.startsWith("add_")) {
						beanProp = beanProp.replace("add_", "");
						List<Object> thisList = new ArrayList<>();
						Object listObj = PropertyUtils.getProperty(bean, beanProp);
						if (listObj instanceof List<?>) {
							thisList.addAll((List<?>) listObj);
						}
						thisList.add(thisInput);
						BeanUtils.setProperty(bean, beanProp, thisList);
					} else {
						BeanUtils.setProperty(bean, beanProp, thisInput);
					}
				}
				// add to the other data
				else {
					BeanUtils.setProperty(bean, "extra", thisInput);
				}
			}
		} catch (IllegalAccessException e) {
			classLogger.error("Failed to set bean property while filling ordered JSON values", e);
		} catch (InvocationTargetException e) {
			classLogger.error("Bean setter threw an error while filling ordered JSON values", e);
		} catch (NoSuchMethodException e) {
			classLogger.error("Bean property method not found while filling ordered JSON values", e);
		}

		return bean;

	}

	/**
	 * Fill a bean from an object-node result.
	 * <p>
	 * Each entry in {@code beanProps} is treated as both:
	 * <ul>
	 * <li>the source key to read from the JSON object node, and</li>
	 * <li>the destination bean property name to write to.</li>
	 * </ul>
	 * Array values are flattened to a comma-separated string. Properties prefixed
	 * with {@code add_} are appended to a list-like destination property.
	 * 
	 * @param result    object-node result from JMES evaluation
	 * @param beanProps source/destination mapping keys
	 * @param bean      target bean instance
	 * @return populated bean
	 */
	public static Object fillSingleObjectFromMap(JsonNode result, String[] beanProps, Object bean) {
		try {
			for (int inputIndex = 0; result != null && inputIndex < beanProps.length; inputIndex++) {
				// grab the bean
				String beanProp = beanProps[inputIndex];

				JsonNode thisInputObj = result.get(beanProp);
				if (thisInputObj.isArray()) {
					// Preserve existing behavior: flatten JSON arrays to a single string.
					StringBuilder concat = new StringBuilder();
					int innerArraySize = thisInputObj.size();
					concat.append(thisInputObj.get(0).asText());
					for (int innerArrayIndex = 1; innerArrayIndex < innerArraySize; innerArrayIndex++) {
						concat.append(", ").append(thisInputObj.get(innerArrayIndex));
					}
					// this is adding as a string
					BeanUtils.setProperty(bean, beanProp, concat.toString());
				} else {
					// grab as string
					String thisInput = thisInputObj.asText();
					if (result.size() > inputIndex) {
						if (beanProp.startsWith("add_")) {
							beanProp = beanProp.replace("add_", "");
							List<Object> thisList = new ArrayList<>();
							Object listObj = PropertyUtils.getProperty(bean, beanProp);
							if (listObj instanceof List<?>) {
								thisList.addAll((List<?>) listObj);
							}
							thisList.add(thisInput);
							BeanUtils.setProperty(bean, beanProp, thisList);
						}
						// normal assignment
						else {
							BeanUtils.setProperty(bean, beanProp, thisInput);
						}
					}
					// fallback destination for extra values
					else {
						BeanUtils.setProperty(bean, "extra", thisInput);
					}
				}
			}
		} catch (IllegalAccessException e) {
			classLogger.error("Failed to set bean property while filling object-node JSON values", e);
		} catch (InvocationTargetException e) {
			classLogger.error("Bean setter threw an error while filling object-node JSON values", e);
		} catch (NoSuchMethodException e) {
			classLogger.error("Bean property method not found while filling object-node JSON values", e);
		}

		return bean;
	}

	/**
	 * Serialize an object to JSON using the shared mapper.
	 * 
	 * @param object object to serialize
	 * @return JSON string
	 * @throws Exception when serialization fails
	 */
	public static String getJson(Object object) throws Exception {
		return mapper.writeValueAsString(object);
	}

}
