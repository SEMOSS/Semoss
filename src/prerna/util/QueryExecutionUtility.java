package prerna.util;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.engine.api.IEngine;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.sql.AbstractSqlQueryUtil;

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
	public static String flushToString(IEngine engine, SelectQueryStruct qs) {
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				return (String) wrapper.next().getValues()[0];
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
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
	public static Integer flushToInteger(IEngine engine, SelectQueryStruct qs) {
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				return (Integer) wrapper.next().getValues()[0];
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		return null;
	}
	
	public static Long flushToLong(IEngine engine, SelectQueryStruct qs) {
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				return (Long) wrapper.next().getValues()[0];
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
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
	public static List<String> flushToListString(IEngine engine, SelectQueryStruct qs) {
		List<String> values = new Vector<String>();
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				values.add(wrapper.next().getValues()[0].toString());
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
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
	public static Set<String> flushToSetString(IEngine engine, SelectQueryStruct qs, boolean order) {
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
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
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
	public static Set<String> flushToSetString(IEngine engine, String query, boolean order) {
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
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		return values;
	}
	
	public static List<String[]> flushRsToListOfStrArray(IEngine engine, SelectQueryStruct qs) {
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
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		return ret;
	}
	
	public static List<Object[]> flushRsToListOfObjArray(IEngine engine, SelectQueryStruct qs) {
		List<Object[]> ret = new ArrayList<Object[]>();
		
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				ret.add(wrapper.next().getValues());
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		return ret;
	}
	
	@Deprecated
	static List<Object[]> flushRsToMatrix(IEngine engine, SelectQueryStruct qs) {
		List<Object[]> ret = new ArrayList<Object[]>();
		
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				ret.add(wrapper.next().getValues());
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		return ret;
	}
	
	public static List<Map<String, Object>> flushRsToMap(IEngine engine, SelectQueryStruct qs) {
		List<Map<String, Object>> result = new ArrayList<Map<String, Object>>();
		
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(engine, qs);
			while(wrapper.hasNext()) {
				IHeadersDataRow headerRow = wrapper.next();
				String[] headers = headerRow.getHeaders();
				Object[] values = headerRow.getValues();
				Map<String, Object> map = new HashMap<String, Object>();
				for(int i = 0; i < headers.length; i++) {
					if(values[i] instanceof java.sql.Clob) {
						String value = AbstractSqlQueryUtil.flushClobToString((java.sql.Clob) values[i]);
						map.put(headers[i], value);
					} else {
						map.put(headers[i], values[i]);
					}
				}
				result.add(map);
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(wrapper != null) {
				wrapper.cleanUp();
			}
		}
		
		return result;
	}
}
