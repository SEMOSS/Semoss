package prerna.auth;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;

import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.Utility;

public abstract class AbstractSecurityUtils {

	static RDBMSNativeEngine securityDb;
	
	/**
	 * Only used for static references
	 */
	AbstractSecurityUtils() {
		
	}
	
	public static void loadSecurityDatabase() {
		securityDb = (RDBMSNativeEngine) Utility.getEngine(Constants.SECURITY_DB);
		// TODO: testing code!!!!
		// TODO: testing code!!!!
		// TODO: testing code!!!!
		// TODO: testing code!!!!
		// TODO: testing code!!!!
		// TODO: testing code!!!!
		// TODO: testing code!!!!
		// TODO: testing code!!!!
		String deleteQuery = "DELETE FROM ENGINE WHERE 1-1";
		securityDb.removeData(deleteQuery);
		deleteQuery = "DELETE FROM INSIGHT WHERE 1-1";
		securityDb.removeData(deleteQuery);
		deleteQuery = "DELETE FROM ENGINEPERMISSION WHERE 1-1";
		securityDb.removeData(deleteQuery);
		deleteQuery = "DELETE FROM ENGINEMETA WHERE 1-1";
		securityDb.removeData(deleteQuery);
	}

	/**
	 * Does this engine name already exist
	 * @param appName
	 * @return
	 */
	@Deprecated
	//TODO: needs to account for a user having the app name already
	public static boolean containsEngine(String appName) {
		if(appName.equals(Constants.LOCAL_MASTER_DB_NAME) || appName.equals(Constants.SECURITY_DB)) {
			// dont add local master or security db to security db
			return true;
		}
		String query = "SELECT ID FROM ENGINE WHERE NAME='" + appName + "'";
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		try {
			if(wrapper.hasNext()) {
				return true;
			} else {
				return false;
			}
		} finally {
			wrapper.cleanUp();
		}
	}
	
	
	
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	///////////////////////////////////////////////////////////////////////////////////
	
	
	/*
	 * Utility methods
	 */
	
	/**
	 * Utility method to flush result set into list
	 * Assumes single return at index 0
	 * @param wrapper
	 * @return
	 */
	static List<String> flushToListString(IRawSelectWrapper wrapper) {
		List<String> values = new Vector<String>();
		while(wrapper.hasNext()) {
			values.add(wrapper.next().getValues()[0].toString());
		}
		return values;
	}
	
	/**
	 * Utility method to flush result set into set
	 * Assumes single return at index 0
	 * @param wrapper
	 * @return
	 */
	static Set<String> flushToSetString(IRawSelectWrapper wrapper, boolean order) {
		Set<String> values = null;
		if(order) {
			values = new TreeSet<String>();
		} else {
			values = new HashSet<String>();
		}
		while(wrapper.hasNext()) {
			values.add(wrapper.next().getValues()[0].toString());
		}
		return values;
	}
	
	static List<Map<String, Object>> flushRsToMap(IRawSelectWrapper wrapper) {
		List<Map<String, Object>> result = new Vector<Map<String, Object>>();
		while(wrapper.hasNext()) {
			IHeadersDataRow headerRow = wrapper.next();
			String[] headers = headerRow.getHeaders();
			Object[] values = headerRow.getValues();
			Map<String, Object> map = new HashMap<String, Object>();
			for(int i = 0; i < headers.length; i++) {
				map.put(headers[i], values[i]);
			}
			result.add(map);
		}
		return result;
	}
	
	static String createFilter(String... filterValues) {
		StringBuilder b = new StringBuilder();
		if(filterValues.length > 0) {
			b.append(" IN (");
			b.append("'").append(filterValues[0]).append("'");
			for(int i = 1; i < filterValues.length; i++) {
				b.append(", '").append(filterValues[i]).append("'");
			}
		}
		b.append(")");
		return b.toString();
	}
}
