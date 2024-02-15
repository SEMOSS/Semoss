package prerna.auth.utils;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.gson.Gson;

import prerna.auth.User;
import prerna.engine.api.IRawSelectWrapper;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.util.Constants;

public class SecurityGroupUtils extends AbstractSecurityUtils {
	
	private static SecurityGroupUtils instance = new SecurityGroupUtils();
	
	private static final Logger classLogger = LogManager.getLogger(SecurityGroupUtils.class);

	private SecurityGroupUtils() {
		
	}
	
	public static SecurityGroupUtils getInstance(User user) {
		if(user == null) {
			return null;
		}
		if(SecurityAdminUtils.userIsAdmin(user)) {
			return instance;
		}
		return null;
	}

	/**
	 * Filter a collection of typed groups to those that are in the SMSS_GROUP table
	 * @param groupIds
	 * @param groupType
	 * @return
	 */
	public static Set<String> getMatchingGroupsByType(Collection<String> groupIds, String groupType) {
		Set<String> results = new HashSet<>();
		
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__ID"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_GROUP__TYPE", "==", groupType));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("SMSS_GROUP__ID", "==", groupIds));
		
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			while(wrapper.hasNext()) {
				Object val = wrapper.next().getValues()[0];
				if(val != null) {
					results.add(val.toString());
				}
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
			throw new IllegalArgumentException("Failed to retrieve matching security groups", e);
		} finally {
			if(wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		
		return results;
	}
	
	/**
	 * Add a group with description
	 * @param groupId
	 * @param type
	 * @param description
	 * @throws Exception 
	 */
	public void addGroup(String groupId, String type, String description, boolean isCustomGroup) throws Exception {
		Connection conn = null;
		try {
			conn = securityDb.makeConnection();
			
			// need to ensure that the group is unique...
			boolean foundGroup = false;
			String getGroupCount = "SELECT COUNT(*) FROM SMSS_GROUP WHERE ID=? AND TYPE=?";
			try(PreparedStatement ps = conn.prepareStatement(getGroupCount)) {
				ps.setString(1, groupId);
				ps.setString(2, type);
				if(ps.execute()) {
					ResultSet rs = ps.getResultSet();
					if(rs.next()) {
						int resultCount = rs.getInt(1);
						if(resultCount > 0) {
							foundGroup = true;
						}
					}
				}
			}
			
			if(foundGroup) {
				throw new IllegalArgumentException("Group already exists");
			}
			
			Gson gson = new Gson();
			String query = "INSERT INTO SMSS_GROUP (ID, TYPE, DESCRIPTION, IS_CUSTOM_GROUP) VALUES (?,?,?,?)";
			try(PreparedStatement ps = conn.prepareStatement(query)) {
				int parameterIndex = 1;
				ps.setString(parameterIndex++, groupId);
				ps.setString(parameterIndex++, type);
				securityDb.getQueryUtil().handleInsertionOfClob(conn, ps, description, parameterIndex++, gson);
				ps.setBoolean(parameterIndex++, isCustomGroup);
				ps.execute();
				if(!conn.getAutoCommit()) {
					conn.commit();
				}
			}
		} finally {
			if(securityDb.isConnectionPooling() && conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	/**
	 * Delete a new group and its references across the tables
	 * @param groupId
	 * @param type
	 */
	public void deleteGroupAndPropagate(String groupId, String type) {
		String[] queries = new String[] {
			"DELETE FROM GROUPENGINEPERMISSION WHERE ID=? AND TYPE=?",
			"DELETE FROM GROUPPROJECTPERMISSION WHERE ID=? AND TYPE=?",
			"DELETE FROM GROUPINSIGHTPERMISSION WHERE ID=? AND TYPE=?",
			"DELETE FROM SMSS_GROUP WHERE ID= ? AND TYPE = ?",
		};
		
		Connection conn = null;
		try {
			conn = securityDb.makeConnection();
			try {
				for(String query : queries) {
					try(PreparedStatement ps = conn.prepareStatement(query)) {
						int parameterIndex = 1;
						ps.setString(parameterIndex++, groupId);
						ps.setString(parameterIndex++, type);
						ps.execute();
					}
				}
				if(!conn.getAutoCommit()) {
					conn.commit();
				}
			} catch(SQLException e) {
				if(!conn.getAutoCommit()) {
					conn.rollback();
				}
				throw e;
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(securityDb.isConnectionPooling() && conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}
	
	/**
	 * Edit an existing group across all the tables
	 * @param curGroupId
	 * @param curType
	 * @param newGroupId
	 * @param newType
	 * @param newDescription
	 * @param newIsCustomGroup
	 * @throws Exception
	 */
	public void editGroupAndPropagate(String curGroupId, String curType, String newGroupId, String newType, String newDescription, boolean newIsCustomGroup) throws Exception{
		String groupQuery = "UPDATE SMSS_GROUP SET ID=?, TYPE=?, DESCRIPTION=?, IS_CUSTOM_GROUP=? WHERE ID=? AND TYPE=?";
		String[] propagateQueries = new String[] {
			"UPDATE GROUPENGINEPERMISSION SET ID=?, TYPE=? WHERE ID=? AND TYPE=?",
			"UPDATE GROUPPROJECTPERMISSION SET ID=?, TYPE=? WHERE ID=? AND TYPE=?",
			"UPDATE GROUPINSIGHTPERMISSION SET ID=?, TYPE=? WHERE ID=? AND TYPE=?"
		};

		Connection conn = null;
		try {
			conn = securityDb.makeConnection();
			
			Gson gson = new Gson();
			try {
				// group edit
				try(PreparedStatement ps = conn.prepareStatement(groupQuery)) {
					int parameterIndex = 1;
					ps.setString(parameterIndex++, newGroupId);
					ps.setString(parameterIndex++, newType);
					securityDb.getQueryUtil().handleInsertionOfClob(conn, ps, newDescription, parameterIndex++, gson);
					ps.setBoolean(parameterIndex++, newIsCustomGroup);
					ps.setString(parameterIndex++, curGroupId);
					ps.setString(parameterIndex++, curType);
					ps.execute();
				}
				
				// propagation
				for(String query : propagateQueries) {
					try(PreparedStatement ps = conn.prepareStatement(query)) {
						int parameterIndex = 1;
						ps.setString(parameterIndex++, newGroupId);
						ps.setString(parameterIndex++, newType);
						ps.setString(parameterIndex++, curGroupId);
						ps.setString(parameterIndex++, curType);
						ps.execute();
					}
				}
				if(!conn.getAutoCommit()) {
					conn.commit();
				}
			} catch(SQLException e) {
				if(!conn.getAutoCommit()) {
					conn.rollback();
				}
				throw e;
			}
		} catch(Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if(securityDb.isConnectionPooling() && conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	/**
	 * Get all groups
	 * @return
	 */
	public List<Map<String, Object>> getAllGroups() {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__ID"));
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__TYPE"));
		qs.addSelector(new QueryColumnSelector("SMSS_GROUP__DESCRIPTION"));
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_GROUP__TYPE"));
		qs.addOrderBy(new QueryColumnOrderBySelector("SMSS_GROUP__ID"));
		return getSimpleQuery(qs);
	}
	
	
}
