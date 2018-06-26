package prerna.auth;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.Vector;

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
	
	/**
	 * Only used for static references
	 */
	AbstractSecurityUtils() {
		
	}
	
	public static void loadSecurityDatabase() {
		securityDb = (RDBMSNativeEngine) Utility.getEngine(Constants.SECURITY_DB);
		initialize();
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
		if(ignoreEngine(appName)) {
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
	
	public static boolean containsEngineId(String appId) {
		if(ignoreEngine(appId)) {
			// dont add local master or security db to security db
			return true;
		}
		String query = "SELECT ID FROM ENGINE WHERE ID='" + appId + "'";
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
	
	public static void initialize() {
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.SECURITY_DB);
		Connection conn = engine.makeConnection();
		String[] colNames = null;
		String[] types = null;

		// ACCESSREQUEST
		colNames = new String[] { "id", "submittedby", "submittedto", "engine", "permission" };
		types = new String[] { "integer", "varchar(800)", "varchar(800)", "integer", "integer" };
		executeSql(conn, RdbmsQueryBuilder.makeOptionalCreate("ACCESSREQUEST", colNames, types));

		// ENGINE
		colNames = new String[] { "name", "id", "global", "type", "cost" };
		types = new String[] { "varchar(800)", "varchar(800)", "boolean", "varchar(800)", "varchar(800)" };
		executeSql(conn, RdbmsQueryBuilder.makeOptionalCreate("ENGINE", colNames, types));

		// ENGINEGROUPMEMBERVISIBILITY
		colNames = new String[] { "id", "groupenginepermissionid", "groupmembersid", "visibility" };
		types = new String[] { "integer", "integer", "integer", "boolean" };
		executeSql(conn, RdbmsQueryBuilder.makeOptionalCreate("ENGINEGROUPMEMBERVISIBILITY", colNames, types));

		// ENGINEMETA
		colNames = new String[] { "engineid", "key", "value" };
		types = new String[] { "varchar(800)", "varchar(800)", "varchar(800)" };
		executeSql(conn, RdbmsQueryBuilder.makeOptionalCreate("ENGINEMETA", colNames, types));

		// ENGINEPERMISSION
		colNames = new String[] { "id", "user", "permission", "engine", "visibility" };
		types = new String[] { "integer", "varchar(800)", "integer", "varchar(800)", "boolean" };
		executeSql(conn, RdbmsQueryBuilder.makeOptionalCreate("ENGINEPERMISSION", colNames, types));

		// GROUPENGINEPERMISSION
		colNames = new String[] { "groupid", "permission", "engine", "id" };
		types = new String[] { "integer", "integer", "varchar(800)", "integer" };
		executeSql(conn, RdbmsQueryBuilder.makeOptionalCreate("GROUPENGINEPERMISSION", colNames, types));

		// GROUPINSIGHTPERMISSION
		colNames = new String[] { "groupid", "engineid", "insightid" };
		types = new String[] { "integer", "integer", "varchar(800)" };
		executeSql(conn, RdbmsQueryBuilder.makeOptionalCreate("GROUPINSIGHTPERMISSION", colNames, types));

		// GROUPMEMBERS
		colNames = new String[] { "groupid", "memberid", "id" };
		types = new String[] { "integer", "varchar(800)", "integer" };
		executeSql(conn, RdbmsQueryBuilder.makeOptionalCreate("GROUPMEMBERS", colNames, types));

		// GROUPSEEDPERMISSION
		colNames = new String[] { "groupid", "seedid" };
		types = new String[] { "integer", "integer" };
		executeSql(conn, RdbmsQueryBuilder.makeOptionalCreate("GROUPSEEDPERMISSION", colNames, types));

		// INSIGHT
		colNames = new String[] { "engineid", "insightid", "insightname", "global" };
		types = new String[] { "varchar(800)", "varchar(800)", "varchar(800)", "boolean" };
		executeSql(conn, RdbmsQueryBuilder.makeOptionalCreate("INSIGHT", colNames, types));

		// INSIGHTEXECUTION
		colNames = new String[] { "user", "database", "insight", "count", "lastexecuted", "session" };
		types = new String[] { "varchar(800)", "varchar(800)", "varchar(800)", "integer", "date", "varchar(800)" };
		executeSql(conn, RdbmsQueryBuilder.makeOptionalCreate("INSIGHTEXECUTION", colNames, types));

		// PERMISSION
		colNames = new String[] { "id", "name" };
		types = new String[] { "integer", "varchar(800)" };
		executeSql(conn, RdbmsQueryBuilder.makeOptionalCreate("PERMISSION", colNames, types));

		// SEED
		colNames = new String[] { "id", "name", "databaseid", "tablename", "columnname", "rlsvalue", "rlsjavacode", "owner" };
		types = new String[] { "integer", "varchar(800)", "integer", "varchar(800)", "varchar(800)", "varchar(800)", "clob", "varchar(800)" };
		executeSql(conn, RdbmsQueryBuilder.makeOptionalCreate("SEED", colNames, types));

		// USER
		colNames = new String[] { "name", "email", "type", "admin", "id", "password", "salt", "username" };
		types = new String[] { "varchar(800)", "varchar(800)", "varchar(800)", "boolean", "varchar(800)", "varchar(800)", "varchar(800)", "varchar(800)" };
		executeSql(conn, RdbmsQueryBuilder.makeOptionalCreate("USER", colNames, types));

		// USERGROUP
		colNames = new String[] { "id", "name", "owner" };
		types = new String[] { "integer", "varchar(800)", "varchar(800)" };
		executeSql(conn, RdbmsQueryBuilder.makeOptionalCreate("USERGROUP", colNames, types));

		// USERINSIGHTPERMISSION
		colNames = new String[] { "userid", "engineid", "insightid", "permission" };
		types = new String[] { "varchar(800)", "varchar(800)", "varchar(800)", "integer" };
		executeSql(conn, RdbmsQueryBuilder.makeOptionalCreate("USERINSIGHTPERMISSION", colNames, types));

		// USERSEEDPERMISSION
		colNames = new String[] { "userid", "seedid" };
		types = new String[] { "varchar(800)", "integer" };
		executeSql(conn, RdbmsQueryBuilder.makeOptionalCreate("USERSEEDPERMISSION", colNames, types));
	}
	
	private static void executeSql(Connection conn, String sql) {
		Statement stmt = null;
		try {
			stmt = conn.createStatement();
			stmt.execute(sql);
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			try {
				stmt.close();
			} catch (SQLException e) {
				e.printStackTrace();
			}
		}
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
		
		if(layout.equals("area")) {
			return new File(imageDir + "area.png");
		} else if(layout.equals("column")) {
			return new File(imageDir + "bar.png");
		} else if(layout.equals("boxwhisker")) {
			return new File(imageDir + "boxwhisker.png");
		} else if(layout.equals("bubble")) {
			return new File(imageDir + "bubble.png");
		} else if(layout.equals("choropleth")) {
			return new File(imageDir + "choropleth.png");
		} else if(layout.equals("cloud")) {
			return new File(imageDir + "cloud.png");
		} else if(layout.equals("cluster")) {
			return new File(imageDir + "cluster.png");
		} else if(layout.equals("dendrogram")) {
			return new File(imageDir + "dendrogram-echarts.png");
		} else if(layout.equals("funnel")) {
			return new File(imageDir + "funnel.png");
		} else if(layout.equals("gauge")) {
			return new File(imageDir + "gauge.png");
		} else if(layout.equals("graph")) {
			return new File(imageDir + "graph.png");
		} else if(layout.equals("grid")) {
			return new File(imageDir + "grid.png");
		} else if(layout.equals("heatmap")) {
			return new File(imageDir + "heatmap.png");
		} else if(layout.equals("infographic")) {
			return new File(imageDir + "infographic.png");
		} else if(layout.equals("line")) {
			return new File(imageDir + "line.png");
		} else if(layout.equals("map")) {
			return new File(imageDir + "map.png");
		} else if(layout.equals("pack")) {
			return new File(imageDir + "pack.png");
		} else if(layout.equals("parallelcoordinates")) {
			return new File(imageDir + "parallel-coordinates.png");
		} else if(layout.equals("pie")) {
			return new File(imageDir + "pie.png");
		} else if(layout.equals("polar")) {
			return new File(imageDir + "polar-bar.png");
		} else if(layout.equals("radar")) {
			return new File(imageDir + "radar.png");
		} else if(layout.equals("sankey")) {
			return new File(imageDir + "sankey.png");
		} else if(layout.equals("scatter")) {
			return new File(imageDir + "scatter.png");
		} else if(layout.equals("scatterplotmatrix")) {
			return new File(imageDir + "scatter-matrix.png");
		} else if(layout.equals("singleaxiscluster")) {
			return new File(imageDir + "single-axis.png");
		} else if(layout.equals("sunburst")) {
			return new File(imageDir + "sunburst.png");
		} else if(layout.equals("text-widget")) {
			return new File(imageDir + "text-widget.png");
		} else if(layout.equals("treemap")) {
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
}
