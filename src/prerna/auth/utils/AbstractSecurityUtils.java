package prerna.auth.utils;

import java.io.File;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import jodd.util.BCrypt;
import prerna.auth.AuthProvider;
import prerna.auth.User;
import prerna.ds.util.RdbmsQueryBuilder;
import prerna.engine.api.IHeadersDataRow;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;
import prerna.util.DIHelper;
import prerna.util.Utility;

public abstract class AbstractSecurityUtils {

	static RDBMSNativeEngine securityDb;
	static boolean securityEnabled = false;
	static String ADMIN_ADDED_USER = "ADMIN_ADDED_USER";
	
	/**
	 * Only used for static references
	 */
	AbstractSecurityUtils() {
		
	}
	
	public static void loadSecurityDatabase() throws SQLException {
		securityDb = (RDBMSNativeEngine) Utility.getEngine(Constants.SECURITY_DB);
		initialize();
		
		Object security = DIHelper.getInstance().getLocalProp(Constants.SECURITY_ENABLED);
		if(security == null) {
			securityEnabled = false;
		} else {
			securityEnabled = (security instanceof Boolean && ((boolean) security) ) || (Boolean.parseBoolean(security.toString()));
		}
	}

	public static boolean securityEnabled() {
		return securityEnabled;
	}
	
	/**
	 * Does this engine name already exist
	 * @param user
	 * @param appName
	 * @return
	 */
	public static boolean userContainsEngineName(User user, String appName) {
		if(ignoreEngine(appName)) {
			// dont add local master or security db to security db
			return true;
		}
		String userFilters = getUserFilters(user);
		String query = "SELECT * "
				+ "FROM ENGINE "
				+ "INNER JOIN ENGINEPERMISSION ON ENGINE.ENGINEID=ENGINEPERMISSION.ENGINEID "
				+ "WHERE ENGINENAME='" + appName + "' AND PERMISSION IN (1,2) AND ENGINEPERMISSION.USERID IN " + userFilters;
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
	
	public static boolean containsEngineName(String appName) {
		if(ignoreEngine(appName)) {
			// dont add local master or security db to security db
			return true;
		}
		String query = "SELECT ENGINEID FROM ENGINE WHERE ENGINENAME='" + appName + "'";
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
	
	public static boolean containsEngineId(String appId) {
		if(ignoreEngine(appId)) {
			// dont add local master or security db to security db
			return true;
		}
		String query = "SELECT ENGINEID FROM ENGINE WHERE ENGINEID='" + appId + "'";
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
	
	static boolean ignoreEngine(String appId) {
		if(appId.equals(Constants.LOCAL_MASTER_DB_NAME) || appId.equals(Constants.SECURITY_DB)) {
			// dont add local master or security db to security db
			return true;
		}
		return false;
	}
	
	public static void initialize() throws SQLException {
		String[] colNames = null;
		String[] types = null;
		Object[] defaultValues = null;
		/*
		 * Currently used
		 */
		
		// ENGINE
		colNames = new String[] { "enginename", "engineid", "global", "type", "cost" };
		types = new String[] { "varchar(255)", "varchar(255)", "boolean", "varchar(255)", "varchar(255)" };
		securityDb.insertData(RdbmsQueryBuilder.makeOptionalCreate("ENGINE", colNames, types));
		securityDb.insertData("CREATE INDEX IF NOT EXISTS ENGINE_GLOBAL_INDEX ON ENGINE (GLOBAL);");
		securityDb.insertData("CREATE INDEX IF NOT EXISTS ENGINE_ENGINENAME_INDEX ON ENGINE (ENGINENAME);");
		securityDb.insertData("CREATE INDEX IF NOT EXISTS ENGINE_ENGINEID_INDEX ON ENGINE (ENGINEID);");

		// ENGINEMETA
		colNames = new String[] { "engineid", "key", "value" };
		types = new String[] { "varchar(255)", "varchar(255)", "varchar(255)" };
		securityDb.insertData(RdbmsQueryBuilder.makeOptionalCreate("ENGINEMETA", colNames, types));

		// ENGINEPERMISSION
		colNames = new String[] { "userid", "permission", "engineid", "visibility" };
		types = new String[] { "varchar(255)", "integer", "varchar(255)", "boolean" };
		defaultValues = new Object[]{null, null, null, true};
		securityDb.insertData(RdbmsQueryBuilder.makeOptionalCreateWithDefault("ENGINEPERMISSION", colNames, types, defaultValues));
		securityDb.insertData("CREATE INDEX IF NOT EXISTS ENGINEPERMISSION_PERMISSION_INDEX ON ENGINEPERMISSION (PERMISSION);");
		securityDb.insertData("CREATE INDEX IF NOT EXISTS ENGINEPERMISSION_VISIBILITY_INDEX ON ENGINEPERMISSION (VISIBILITY);");
		securityDb.insertData("CREATE INDEX IF NOT EXISTS ENGINEPERMISSION_ENGINEID_INDEX ON ENGINEPERMISSION (ENGINEID);");

		// WORKSPACEENGINE
		// TODO >>>timb: WORKSPACE - DONE - here are the sec queries for reference
		colNames = new String[] {"type", "userid", "engineid"};
		types = new String[] {"varchar(255)", "varchar(255)", "varchar(255)"};
		securityDb.insertData(RdbmsQueryBuilder.makeOptionalCreate("WORKSPACEENGINE", colNames, types));
		securityDb.insertData("CREATE INDEX IF NOT EXISTS WORKSPACEENGINE_TYPE_INDEX ON WORKSPACEENGINE (TYPE);");
		securityDb.insertData("CREATE INDEX IF NOT EXISTS WORKSPACEENGINE_USERID_INDEX ON WORKSPACEENGINE (USERID);");

		// ASSETENGINE
		colNames = new String[] {"type", "userid", "engineid"};
		types = new String[] {"varchar(255)", "varchar(255)", "varchar(255)"};
		securityDb.insertData(RdbmsQueryBuilder.makeOptionalCreate("ASSETENGINE", colNames, types));
		securityDb.insertData("CREATE INDEX IF NOT EXISTS ASSETENGINE_TYPE_INDEX ON WORKSPACEENGINE (TYPE);");
		securityDb.insertData("CREATE INDEX IF NOT EXISTS ASSETENGINE_USERID_INDEX ON WORKSPACEENGINE (USERID);");
		
		// INSIGHT
		colNames = new String[] { "engineid", "insightid", "insightname", "global", "executioncount", "createdon", "lastmodifiedon", "layout", "cacheable" };
		types = new String[] { "varchar(255)", "varchar(255)", "varchar(255)", "boolean", "bigint", "timestamp", "timestamp", "varchar(255)", "boolean" };
		securityDb.insertData(RdbmsQueryBuilder.makeOptionalCreate("INSIGHT", colNames, types));
		securityDb.insertData("CREATE INDEX IF NOT EXISTS INSIGHT_LASTMODIFIEDON_INDEX ON INSIGHT (LASTMODIFIEDON);");
		securityDb.insertData("CREATE INDEX IF NOT EXISTS INSIGHT_GLOBAL_INDEX ON INSIGHT (GLOBAL);");
		securityDb.insertData("CREATE INDEX IF NOT EXISTS INSIGHT_ENGINEID_INDEX ON INSIGHT (ENGINEID);");
//		securityDb.insertData("CREATE INDEX IF NOT EXISTS INSIGHT_INSIGHTNAME_INDEX ON INSIGHT (INSIGHTNAME);");
//		securityDb.insertData("CREATE INDEX IF NOT EXISTS INSIGHT_INSIGHTID_INDEX ON INSIGHT (INSIGHTID);");

		// USERINSIGHTPERMISSION
		colNames = new String[] { "userid", "engineid", "insightid", "permission" };
		types = new String[] { "varchar(255)", "varchar(255)", "varchar(255)", "integer" };
		securityDb.insertData(RdbmsQueryBuilder.makeOptionalCreate("USERINSIGHTPERMISSION", colNames, types));
		securityDb.insertData("CREATE INDEX IF NOT EXISTS USERINSIGHTPERMISSION_PERMISSION_INDEX ON USERINSIGHTPERMISSION (PERMISSION);");
		securityDb.insertData("CREATE INDEX IF NOT EXISTS USERINSIGHTPERMISSION_ENGINEID_INDEX ON USERINSIGHTPERMISSION (ENGINEID);");
		securityDb.insertData("CREATE INDEX IF NOT EXISTS USERINSIGHTPERMISSION_USERID_INDEX ON USERINSIGHTPERMISSION (USERID);");

		// PERMISSION
		colNames = new String[] { "id", "name" };
		types = new String[] { "integer", "varchar(255)" };
		securityDb.insertData(RdbmsQueryBuilder.makeOptionalCreate("PERMISSION", colNames, types));
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, "select count(*) from permission");
		if(wrapper.hasNext()) {
			int numrows = ((Number) wrapper.next().getValues()[0]).intValue();
			if(numrows > 3) {
				securityDb.removeData("DELETE FROM PERMISSION WHERE 1=1;");
				securityDb.insertData(RdbmsQueryBuilder.makeInsert("PERMISSION", colNames, types, new Object[]{1, "OWNER"}));
				securityDb.insertData(RdbmsQueryBuilder.makeInsert("PERMISSION", colNames, types, new Object[]{2, "EDIT"}));
				securityDb.insertData(RdbmsQueryBuilder.makeInsert("PERMISSION", colNames, types, new Object[]{3, "READ_ONLY"}));
			} else if(numrows == 0) {
				securityDb.insertData(RdbmsQueryBuilder.makeInsert("PERMISSION", colNames, types, new Object[]{1, "OWNER"}));
				securityDb.insertData(RdbmsQueryBuilder.makeInsert("PERMISSION", colNames, types, new Object[]{2, "EDIT"}));
				securityDb.insertData(RdbmsQueryBuilder.makeInsert("PERMISSION", colNames, types, new Object[]{3, "READ_ONLY"}));
			}
		}
		securityDb.insertData("CREATE INDEX IF NOT EXISTS PERMISSION_ID_NAME_INDEX ON PERMISSION (ID, NAME);");

		// ACCESSREQUEST
		colNames = new String[] { "id", "submittedby", "engine", "permission" };
		types = new String[] { "varchar(255)", "varchar(255)", "varchar(255)", "integer" };
		securityDb.insertData(RdbmsQueryBuilder.makeOptionalCreate("ACCESSREQUEST", colNames, types));
		
		// THIS IS FOR LEGACY !!!!
		// TODO: EVENTUALLY WE WILL DELETE THIS
		// TODO: EVENTUALLY WE WILL DELETE THIS
		// TODO: EVENTUALLY WE WILL DELETE THIS
		// TODO: EVENTUALLY WE WILL DELETE THIS
		// TODO: EVENTUALLY WE WILL DELETE THIS
		
		// ADD NEW COLUMN FOR CACHEABLE INSIGHTS
		securityDb.insertData("ALTER TABLE INSIGHT ADD COLUMN IF NOT EXISTS CACHEABLE BOOLEAN DEFAULT TRUE");
		
		// DROP COLUMN SUBMITTEDTO IN TABLE ACCESSREQUEST
		securityDb.removeData("ALTER TABLE ACCESSREQUEST DROP COLUMN IF EXISTS SUBMITTEDTO");
		// CHANGE TYPES TO BE STRINGS WHERE APPROPRIATE
		wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, "SELECT COLUMN_NAME, TYPE_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME='ACCESSREQUEST' and COLUMN_NAME IN ('ID','ENGINE')");
		while(wrapper.hasNext()) {
			Object[] row = wrapper.next().getValues();
			String column = row[0].toString();
			String type = row[1].toString();
			if(!type.equals("VARCHAR")) {
				securityDb.insertData("ALTER TABLE ACCESSREQUEST ALTER COLUMN " + column + " VARCHAR(255);");
			}
		}
		wrapper.cleanUp();
		
		////////////////////////////////////////////////////////////////////
		////////////////////////////////////////////////////////////////////
		////////////////////////////////////////////////////////////////////
		////////////////////////////////////////////////////////////////////
		
		/*
		 * Tables accounted for that we are not using yet...
		 */
		
		// USER
		colNames = new String[] { "name", "email", "type", "admin", "id", "password", "salt", "username" };
		types = new String[] { "varchar(255)", "varchar(255)", "varchar(255)", "boolean", "varchar(255)", "varchar(255)", "varchar(255)", "varchar(255)" };
		securityDb.insertData(RdbmsQueryBuilder.makeOptionalCreate("USER", colNames, types));
		
		// USERGROUP
		colNames = new String[] { "groupid", "name", "owner" };
		types = new String[] { "int identity", "varchar(255)", "varchar(255)" };
		securityDb.insertData(RdbmsQueryBuilder.makeOptionalCreate("USERGROUP", colNames, types));
		
		// GROUPMEMBERS
		colNames = new String[] {"groupmembersid", "groupid", "userid"};
		types = new String[] {"int identity", "integer", "varchar(255)"};
		securityDb.insertData(RdbmsQueryBuilder.makeOptionalCreate("GROUPMEMBERS", colNames, types));
		
		// ENGINEGROUPMEMBERVISIBILITY
		colNames = new String[] { "id", "groupenginepermissionid", "groupmembersid", "visibility" };
		types = new String[] { "int identity", "integer", "integer", "boolean" };
		defaultValues = new Object[]{null, null, null, true};
		securityDb.insertData(RdbmsQueryBuilder.makeOptionalCreateWithDefault("ENGINEGROUPMEMBERVISIBILITY", colNames, types, defaultValues));

		// GROUPENGINEPERMISSION
		colNames = new String[] {"groupenginepermissionid", "groupid", "permission", "engine"};
		types = new String[] {"int identity", "integer", "integer", "varchar(255)"};
		securityDb.insertData(RdbmsQueryBuilder.makeOptionalCreate("GROUPENGINEPERMISSION", colNames, types));
		
		// FOREIGN KEYS FOR CASCASDE DELETE
		wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, "select count(*) from INFORMATION_SCHEMA.CONSTRAINTS where constraint_name='FK_GROUPENGINEPERMISSION'");
		if(wrapper.hasNext()) {
			if( ((Number) wrapper.next().getValues()[0]).intValue() == 0) {
				securityDb.insertData("ALTER TABLE ENGINEGROUPMEMBERVISIBILITY ADD CONSTRAINT FK_GROUPENGINEPERMISSION FOREIGN KEY (GROUPENGINEPERMISSIONID) REFERENCES GROUPENGINEPERMISSION(GROUPENGINEPERMISSIONID) ON DELETE CASCADE;");
			}
		}
		wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, "select count(*) from INFORMATION_SCHEMA.CONSTRAINTS where constraint_name='FK_GROUPMEMBERSID'");
		if(wrapper.hasNext()) {
			if( ((Number) wrapper.next().getValues()[0]).intValue() == 0) {
				securityDb.insertData("ALTER TABLE ENGINEGROUPMEMBERVISIBILITY ADD CONSTRAINT FK_GROUPMEMBERSID FOREIGN KEY (GROUPMEMBERSID) REFERENCES GROUPMEMBERS (GROUPMEMBERSID) ON DELETE CASCADE;");
			}
		}
				
//		// GROUPINSIGHTPERMISSION
//		colNames = new String[] { "groupid", "engineid", "insightid" };
//		types = new String[] { "integer", "integer", "varchar(255)" };
//		securityDb.insertData(RdbmsQueryBuilder.makeOptionalCreate("GROUPINSIGHTPERMISSION", colNames, types));

//		// INSIGHTEXECUTION
//		colNames = new String[] { "user", "database", "insight", "count", "lastexecuted", "session" };
//		types = new String[] { "varchar(255)", "varchar(255)", "varchar(255)", "integer", "date", "varchar(255)" };
//		securityDb.insertData(RdbmsQueryBuilder.makeOptionalCreate("INSIGHTEXECUTION", colNames, types));

//		// SEED
//		colNames = new String[] { "id", "name", "databaseid", "tablename", "columnname", "rlsvalue", "rlsjavacode", "owner" };
//		types = new String[] { "integer", "varchar(255)", "integer", "varchar(255)", "varchar(255)", "varchar(255)", "clob", "varchar(255)" };
//		securityDb.insertData(RdbmsQueryBuilder.makeOptionalCreate("SEED", colNames, types));

//		// USERSEEDPERMISSION
//		colNames = new String[] { "userid", "seedid" };
//		types = new String[] { "varchar(255)", "integer" };
//		securityDb.insertData(RdbmsQueryBuilder.makeOptionalCreate("USERSEEDPERMISSION", colNames, types));
		
//		// GROUPSEEDPERMISSION
//		colNames = new String[] { "groupid", "seedid" };
//		types = new String[] { "integer", "integer" };
//		securityDb.insertData(RdbmsQueryBuilder.makeOptionalCreate("GROUPSEEDPERMISSION", colNames, types));
	}
	
	/**
	 * Get default image for insight
	 * @param appId
	 * @param insightId
	 * @return
	 */
	public static File getStockImage(String appId, String insightId) {
		String imageDir = DIHelper.getInstance().getProperty(Constants.BASE_FOLDER) + "/images/stock/";
		String query = "SELECT LAYOUT FROM INSIGHT WHERE INSIGHT.ENGINEID='" + appId + "' AND INSIGHT.INSIGHTID='" + insightId + "'";
		String layout = null;
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		try {
			while(wrapper.hasNext()) {
				layout = wrapper.next().getValues()[0].toString();
			} 
		} finally {
			wrapper.cleanUp();
		}
		
		if(layout == null) {
			return null;
		}
		
		if(layout.equalsIgnoreCase("area")) {
			return new File(imageDir + "area.png");
		} else if(layout.equalsIgnoreCase("column")) {
			return new File(imageDir + "bar.png");
		} else if(layout.equalsIgnoreCase("boxwhisker")) {
			return new File(imageDir + "boxwhisker.png");
		} else if(layout.equalsIgnoreCase("bubble")) {
			return new File(imageDir + "bubble.png");
		} else if(layout.equalsIgnoreCase("choropleth")) {
			return new File(imageDir + "choropleth.png");
		} else if(layout.equalsIgnoreCase("cloud")) {
			return new File(imageDir + "cloud.png");
		} else if(layout.equalsIgnoreCase("cluster")) {
			return new File(imageDir + "cluster.png");
		} else if(layout.equalsIgnoreCase("dendrogram")) {
			return new File(imageDir + "dendrogram-echarts.png");
		} else if(layout.equalsIgnoreCase("funnel")) {
			return new File(imageDir + "funnel.png");
		} else if(layout.equalsIgnoreCase("gauge")) {
			return new File(imageDir + "gauge.png");
		} else if(layout.equalsIgnoreCase("graph")) {
			return new File(imageDir + "graph.png");
		} else if(layout.equalsIgnoreCase("grid")) {
			return new File(imageDir + "grid.png");
		} else if(layout.equalsIgnoreCase("heatmap")) {
			return new File(imageDir + "heatmap.png");
		} else if(layout.equalsIgnoreCase("infographic")) {
			return new File(imageDir + "infographic.png");
		} else if(layout.equalsIgnoreCase("line")) {
			return new File(imageDir + "line.png");
		} else if(layout.equalsIgnoreCase("map")) {
			return new File(imageDir + "map.png");
		} else if(layout.equalsIgnoreCase("pack")) {
			return new File(imageDir + "pack.png");
		} else if(layout.equalsIgnoreCase("parallelcoordinates")) {
			return new File(imageDir + "parallel-coordinates.png");
		} else if(layout.equalsIgnoreCase("pie")) {
			return new File(imageDir + "pie.png");
		} else if(layout.equalsIgnoreCase("polar")) {
			return new File(imageDir + "polar-bar.png");
		} else if(layout.equalsIgnoreCase("radar")) {
			return new File(imageDir + "radar.png");
		} else if(layout.equalsIgnoreCase("sankey")) {
			return new File(imageDir + "sankey.png");
		} else if(layout.equalsIgnoreCase("scatter")) {
			return new File(imageDir + "scatter.png");
		} else if(layout.equalsIgnoreCase("scatterplotmatrix")) {
			return new File(imageDir + "scatter-matrix.png");
		} else if(layout.equalsIgnoreCase("singleaxiscluster")) {
			return new File(imageDir + "single-axis.png");
		} else if(layout.equalsIgnoreCase("sunburst")) {
			return new File(imageDir + "sunburst.png");
		} else if(layout.equalsIgnoreCase("text-widget")) {
			return new File(imageDir + "text-widget.png");
		} else if(layout.equalsIgnoreCase("treemap")) {
			return new File(imageDir + "treemap.png");
		} else {
			return new File(imageDir + "color-logo.png");
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
	static String flushToString(IRawSelectWrapper wrapper) {
		try {
			while(wrapper.hasNext()) {
				return wrapper.next().getValues()[0].toString();
			}
		} finally {
			wrapper.cleanUp();
		}
		return null;
	}
	
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
	
	static List<String[]> flushRsToListOfStrArray(IRawSelectWrapper wrapper) {
		List<String[]> ret = new ArrayList<String[]>();
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
		return ret;
	}
	
	static List<Object[]> flushRsToMatrix(IRawSelectWrapper wrapper) {
		List<Object[]> ret = new ArrayList<Object[]>();
		while(wrapper.hasNext()) {
			ret.add(wrapper.next().getValues());
		}
		return ret;
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
		boolean hasData = false;
		if(filterValues.length > 0) {
			hasData = true;
			b.append(" IN (");
			b.append("'").append(filterValues[0]).append("'");
			for(int i = 1; i < filterValues.length; i++) {
				b.append(", '").append(filterValues[i]).append("'");
			}
		}
		if(hasData) {
			b.append(")");
		}
		return b.toString();
	}
	
	static String createFilter(Collection<String> filterValues) {
		if(filterValues.isEmpty()) {
			return " IN () ";
		}
		StringBuilder b = new StringBuilder();
		boolean hasData = false;
		if(filterValues.size() > 0) {
			hasData = true;
			b.append(" IN (");
			Iterator<String> iterator = filterValues.iterator();
			b.append("'").append(iterator.next()).append("'");
			while(iterator.hasNext()) {
				b.append(", '").append(iterator.next()).append("'");
			}
		}
		if(hasData) {
			b.append(")");
		}
		return b.toString();
	}
	
	static String createFilter(String firstValue, String... filterValues) {
		StringBuilder b = new StringBuilder();
		b.append(" IN (");
		b.append("'").append(firstValue).append("'");
		for(int i = 0; i < filterValues.length; i++) {
			b.append(", '").append(filterValues[i]).append("'");
		}
		b.append(")");
		return b.toString();
	}
	
	/**
	 * Get all ids from user object
	 * @param user
	 * @return
	 */
	static String getUserFilters(User user) {
		StringBuilder b = new StringBuilder();
		b.append("(");
		if(user != null) {
			List<AuthProvider> logins = user.getLogins();
			if(!logins.isEmpty()) {
				int numLogins = logins.size();
				b.append("'").append(RdbmsQueryBuilder.escapeForSQLStatement(user.getAccessToken(logins.get(0)).getId())).append("'");
				for(int i = 1; i < numLogins; i++) {
					b.append(", '").append(RdbmsQueryBuilder.escapeForSQLStatement(user.getAccessToken(logins.get(i)).getId())).append("'");
				}
			}
		}
		b.append(")");
		return b.toString();
	}
	
	////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////
	
	/**
	 * Returns a list of values given a query with one column/variable.
	 * @param query		Query to be executed to retrieve engine names
	 * @return			List of engine names
	 */
	static List<Map<String, Object>> getSimpleQuery(String query) {
		IRawSelectWrapper wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, query);
		List<Map<String, Object>> ret = new Vector<Map<String, Object>>();
		while(wrapper.hasNext()) {
			IHeadersDataRow row = wrapper.next();
			String[] headers = row.getHeaders();
			Object[] values = row.getValues();
			Map<String, Object> rowData = new HashMap<String, Object>();
			for(int idx = 0; idx < headers.length; idx++){
				if(values[idx] == null) {
					rowData.put(headers[idx].toLowerCase(), "null");
				} else {
					if(headers[idx].toLowerCase().equals("type") && values[idx].toString().equals("NATIVE")){
						rowData.put(headers[idx].toLowerCase(), "Default");
					} else {
						rowData.put(headers[idx].toLowerCase(), values[idx]);
					}
				}
			}
			ret.add(rowData);
		}

		return ret;
	}
	
	
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////
	////////////////////////////////////////////////////////////////////////

	static String escapeRegexCharacters(String s) {
		s = s.trim();
		s = s.replace("(", "\\(");
		s = s.replace(")", "\\)");
		return s;
	}
	
	static String validEmail(String email){
		if(!email.matches("^[^\\s@]+@[^\\s@]+\\.[^\\s@]{2,}$")){
			return  email + " is not a valid email address. ";
		}
		return "";
	}
	
	static String validPassword(String password){
		Pattern pattern = Pattern.compile("^(?=.*[a-z])(?=.*[A-Z])(?=.*[0-9])(?=.*[!@#$%^&*])(?=.{8,})");
        Matcher matcher = pattern.matcher(password);
		
		if(!matcher.lookingAt()){
			return "Password doesn't comply with the security policies.";
		}
		return "";
	}
	
	/**
	 * Current salt generation by BCrypt
	 * @return salt
	 */
	static String generateSalt(){
		return BCrypt.gensalt();
	}

	/**
	 * Create the password hash based on the password and salt provided.
	 * @param password
	 * @param salt
	 * @return hash
	 */
	static String hash(String password, String salt) {
		return BCrypt.hashpw(password, salt);
	}
}
