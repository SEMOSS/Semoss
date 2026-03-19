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

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.ToNumberPolicy;
import com.google.gson.reflect.TypeToken;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.sql.AbstractSqlQueryUtil;

public class QueryExecutionUtility {

	private static final Logger classLogger = LogManager.getLogger(QueryExecutionUtility.class);

	private static final Gson GSON = new GsonBuilder().setObjectToNumberStrategy(ToNumberPolicy.LONG_OR_DOUBLE)
			.disableHtmlEscaping().create();

	private QueryExecutionUtility() {

	}

	/*
	 * Utility methods
	 */

	/**
	 * Utility method to flush result set into list Assumes single return at index 0
	 * 
	 * @param wrapper
	 * @return
	 */
	public static String flushToString(IDatabaseEngine engine, SelectQueryStruct qs) {
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs)) {
			while (wrapper.hasNext()) {
				return (String) wrapper.next().getValues()[0];
			}
		} catch (Exception e) {
			classLogger.error("Error flushing query result to String", e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		}

		return null;
	}

	/**
	 * Utility method to flush result set into an integer Assumes single return at
	 * index 0
	 * 
	 * @param wrapper
	 * @return
	 */
	public static Integer flushToInteger(IDatabaseEngine engine, SelectQueryStruct qs) {
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs)) {
			while (wrapper.hasNext()) {
				Number val = ((Number) wrapper.next().getValues()[0]);
				if (val != null) {
					return val.intValue();
				}
			}
		} catch (Exception e) {
			classLogger.error("Error flushing query result to Integer", e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		}

		return null;
	}

	public static Long flushToLong(IDatabaseEngine engine, SelectQueryStruct qs) {
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs)) {
			while (wrapper.hasNext()) {
				Number val = ((Number) wrapper.next().getValues()[0]);
				if (val != null) {
					return val.longValue();
				}
			}
		} catch (Exception e) {
			classLogger.error("Error flushing query result to Long", e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		}

		return null;
	}

	/**
	 * Utility method to flush result set into list Assumes single return at index 0
	 * 
	 * @param wrapper
	 * @return
	 */
	public static List<String> flushToListString(IDatabaseEngine engine, SelectQueryStruct qs) {
		List<String> values = new ArrayList<String>();
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs)) {
			while (wrapper.hasNext()) {
				values.add(wrapper.next().getValues()[0].toString());
			}
		} catch (Exception e) {
			classLogger.error("Error flushing query result to List<String>", e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		}

		return values;
	}

	/**
	 * Utility method to flush result set into set Assumes single return at index 0
	 * 
	 * @param wrapper
	 * @return
	 */
	public static Set<String> flushToSetString(IDatabaseEngine engine, SelectQueryStruct qs, boolean order) {
		Set<String> values = null;
		if (order) {
			values = new TreeSet<String>();
		} else {
			values = new HashSet<String>();
		}
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs)) {
			while (wrapper.hasNext()) {
				values.add(wrapper.next().getValues()[0].toString());
			}
		} catch (Exception e) {
			classLogger.error("Error flushing query result to Set<String>", e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		}

		return values;
	}

	/**
	 * Utility method to flush result set into set Assumes single return at index 0
	 * 
	 * @param wrapper
	 * @return
	 */
	public static Set<String> flushToSetString(IDatabaseEngine engine, String query, boolean order) {
		Set<String> values = null;
		if (order) {
			values = new TreeSet<String>();
		} else {
			values = new HashSet<String>();
		}
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, query)) {
			while (wrapper.hasNext()) {
				values.add(wrapper.next().getValues()[0].toString());
			}
		} catch (Exception e) {
			classLogger.error("Error flushing raw query result to Set<String>", e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		}

		return values;
	}

	public static List<String[]> flushRsToListOfStrArray(IDatabaseEngine engine, SelectQueryStruct qs) {
		List<String[]> ret = new ArrayList<String[]>();

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs)) {
			while (wrapper.hasNext()) {
				IHeadersDataRow headerRow = wrapper.next();
				Object[] values = headerRow.getValues();
				int len = values.length;
				String[] strVals = new String[len];
				for (int i = 0; i < len; i++) {
					strVals[i] = values[i] + "";
				}
				ret.add(strVals);
			}
		} catch (Exception e) {
			classLogger.error("Error flushing query result set to List<String[]>", e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		}

		return ret;
	}

	public static List<Object[]> flushRsToListOfObjArray(IDatabaseEngine engine, SelectQueryStruct qs) {
		List<Object[]> ret = new ArrayList<Object[]>();

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs)) {
			while (wrapper.hasNext()) {
				ret.add(wrapper.next().getValues());
			}
		} catch (Exception e) {
			classLogger.error("Error flushing query result set to List<Object[]>", e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		}

		return ret;
	}

	@Deprecated
	static List<Object[]> flushRsToMatrix(IDatabaseEngine engine, SelectQueryStruct qs) {
		List<Object[]> ret = new ArrayList<Object[]>();

		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs)) {
			while (wrapper.hasNext()) {
				ret.add(wrapper.next().getValues());
			}
		} catch (Exception e) {
			classLogger.error("Error flushing query result set to matrix", e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		}

		return ret;
	}

	/**
	 * 
	 * @param engine
	 * @param qs
	 * @return
	 */
	public static List<Map<String, Object>> flushRsToMap(IDatabaseEngine engine, SelectQueryStruct qs) {
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs)) {
			return flushWrapperToMap(wrapper);
		} catch (Exception e) {
			classLogger.error("Error flushing query result set to List<Map>", e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		}
	}

	/**
	 * 
	 * @param engine
	 * @param qs
	 * @param mapKeys
	 * @return
	 */
	public static List<Map<String, Object>> flushRsToMap(IDatabaseEngine engine, SelectQueryStruct qs,
			Set<String> mapKeys) {
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs)) {
			return flushWrapperToMap(wrapper, mapKeys);
		} catch (Exception e) {
			classLogger.error("Error flushing query result set to List<Map> with key filter", e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		}
	}

	/**
	 * Query can only have 2 projections
	 * 
	 * @param engine
	 * @param qs
	 * @return
	 */
	public static Map<Object, Object> flushRsToKeyValueMap(IDatabaseEngine engine, SelectQueryStruct qs) {
		try (IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs)) {
			Map<Object, Object> result = new HashMap<>();
			try {
				while (wrapper.hasNext()) {
					IHeadersDataRow headerRow = wrapper.next();
					Object[] values = headerRow.getValues();
					result.put(values[0], values[1]);
				}
			} catch (Exception e) {
				classLogger.error("Error iterating query result set to key-value Map", e);
				throw new IllegalArgumentException("Error executing query: " + e.getMessage());
			}
			return result;
		} catch (Exception e) {
			classLogger.error("Error flushing query result set to key-value Map", e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		}
	}

	/**
	 * 
	 * @param wrapper
	 * @return
	 */
	public static List<Map<String, Object>> flushWrapperToMap(IRawSelectWrapper wrapper) {
		return flushWrapperToMap(wrapper, null);
	}

	/**
	 * 
	 * @param wrapper
	 * @return
	 */
	public static List<Map<String, Object>> flushWrapperToMap(IRawSelectWrapper wrapper, Set<String> mapKeys) {
		List<Map<String, Object>> result = new ArrayList<>();
		try {
			while (wrapper.hasNext()) {
				IHeadersDataRow headerRow = wrapper.next();
				String[] headers = headerRow.getHeaders();
				Object[] values = headerRow.getValues();
				Map<String, Object> map = new HashMap<String, Object>();
				for (int i = 0; i < headers.length; i++) {
					String value = null;
					if (values[i] instanceof java.sql.Clob) {
						value = AbstractSqlQueryUtil.flushClobToString((java.sql.Clob) values[i]);
					} else if (values[i] instanceof java.sql.Blob) {
						value = AbstractSqlQueryUtil.flushBlobToString((java.sql.Blob) values[i]);
					}
					if (mapKeys != null && mapKeys.contains(headers[i])) {
						Map<String, Object> processedValue = convertJsonString(value == null ? values[i] : value);
						map.put(headers[i], processedValue == null ? values[i] : processedValue);
					} else {
						map.put(headers[i], value == null ? values[i] : value);
					}
				}
				result.add(map);
			}
		} catch (Exception e) {
			classLogger.error("Error flushing wrapper result set to List<Map>", e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		}

		return result;
	}

	/**
	 * 
	 * @param jsonString
	 * @return
	 */
	private static Map<String, Object> convertJsonString(Object jsonString) {
		if (jsonString == null) {
			return null;
		}
		try {
			String json = (String) jsonString;
			Type type = new TypeToken<Map<String, Object>>() {
			}.getType();
			return GSON.fromJson(json, type);
		} catch (Exception e) {
			// Not a valid JSON object return null
			return null;
		}
	}
}
