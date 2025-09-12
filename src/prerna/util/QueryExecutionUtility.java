package prerna.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IDatabaseEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.sql.AbstractSqlQueryUtil;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import java.lang.reflect.Type;

public class QueryExecutionUtility {

	private static final Logger classLogger = LogManager.getLogger(QueryExecutionUtility.class);
	
	private QueryExecutionUtility() {
		
	}
	
	/*
	 * Utility methods
	 */
	
	/**
	 * Utility method to flush result set into list
	 * Assumes single return at index 0
	 * @param wrapper
	 * @return
	 */
	public static String flushToString(IDatabaseEngine engine, SelectQueryStruct qs) {
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				return (String) wrapper.next().getValues()[0];
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		} finally {
			if (wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return null;
	}
	
	/**
	 * Utility method to flush result set into an integer
	 * Assumes single return at index 0
	 * @param wrapper
	 * @return
	 */
	public static Integer flushToInteger(IDatabaseEngine engine, SelectQueryStruct qs) {
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				Number val = ((Number) wrapper.next().getValues()[0]);
				if(val != null) {
					return val.intValue();
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		} finally {
			if (wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return null;
	}
	
	public static Long flushToLong(IDatabaseEngine engine, SelectQueryStruct qs) {
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				Number val = ((Number) wrapper.next().getValues()[0]);
				if(val != null) {
					return val.longValue();
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		} finally {
			if (wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return null;
	}
	
	/**
	 * Utility method to flush result set into list
	 * Assumes single return at index 0
	 * @param wrapper
	 * @return
	 */
	public static List<String> flushToListString(IDatabaseEngine engine, SelectQueryStruct qs) {
		List<String> values = new ArrayList<String>();
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				values.add(wrapper.next().getValues()[0].toString());
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		} finally {
			if (wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return values;
	}
	
	/**
	 * Utility method to flush result set into set
	 * Assumes single return at index 0
	 * @param wrapper
	 * @return
	 */
	public static Set<String> flushToSetString(IDatabaseEngine engine, SelectQueryStruct qs, boolean order) {
		Set<String> values = null;
		if(order) {
			values = new TreeSet<String>();
		} else {
			values = new HashSet<String>();
		}
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				values.add(wrapper.next().getValues()[0].toString());
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		} finally {
			if (wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return values;
	}
	
	/**
	 * Utility method to flush result set into set
	 * Assumes single return at index 0
	 * @param wrapper
	 * @return
	 */
	public static Set<String> flushToSetString(IDatabaseEngine engine, String query, boolean order) {
		Set<String> values = null;
		if(order) {
			values = new TreeSet<String>();
		} else {
			values = new HashSet<String>();
		}
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, query);
			while(wrapper.hasNext()) {
				values.add(wrapper.next().getValues()[0].toString());
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		} finally {
			if (wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return values;
	}
	
	public static List<String[]> flushRsToListOfStrArray(IDatabaseEngine engine, SelectQueryStruct qs) {
		List<String[]> ret = new ArrayList<String[]>();
		
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				IHeadersDataRow headerRow = wrapper.next();
				Object[] values = headerRow.getValues();
				int len = values.length;
				String[] strVals = new String[len];
				for(int i = 0; i < len; i++) {
					strVals[i] = values[i] + "";
				}
				ret.add(strVals);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		} finally {
			if (wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return ret;
	}
	
	public static List<Object[]> flushRsToListOfObjArray(IDatabaseEngine engine, SelectQueryStruct qs) {
		List<Object[]> ret = new ArrayList<Object[]>();
		
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				ret.add(wrapper.next().getValues());
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		} finally {
			if (wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return ret;
	}
	
	@Deprecated
	static List<Object[]> flushRsToMatrix(IDatabaseEngine engine, SelectQueryStruct qs) {
		List<Object[]> ret = new ArrayList<Object[]>();
		
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				ret.add(wrapper.next().getValues());
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		} finally {
			if (wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
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
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			return flushWrapperToMap(wrapper);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
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
	public static List<Map<String, Object>> flushRsToMap(IDatabaseEngine engine, SelectQueryStruct qs, Set<String> mapKeys) {
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			return flushWrapperToMap(wrapper, mapKeys);
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
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
			while(wrapper.hasNext()) {
				IHeadersDataRow headerRow = wrapper.next();
				String[] headers = headerRow.getHeaders();
				Object[] values = headerRow.getValues();
				Map<String, Object> map = new HashMap<String, Object>();
				for(int i = 0; i < headers.length; i++) {
					String value = null;
					if(values[i] instanceof java.sql.Clob) {
						value = AbstractSqlQueryUtil.flushClobToString((java.sql.Clob) values[i]);
					} else if(values[i] instanceof java.sql.Blob) {
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
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Error executing query: " + e.getMessage());
		} finally {
			if (wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return result;
	}
	
	/**
	 * 
	 * @param jsonString
	 * @return
	 */
	private static Map<String, Object> convertJsonString (Object jsonString) {
		if (jsonString == null) {
			return null;
		}
        try {
        	String json = (String) jsonString;
            Gson gson = new Gson();
            Type type = new TypeToken<Map<String, Object>>(){}.getType();
            return gson.fromJson(json, type);
        } catch (Exception e) {
            // Not a valid JSON object return null
        	return null;
        }
    }
}
