package prerna.auth.utils;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import prerna.auth.User;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.selectors.QueryColumnOrderBySelector;
import prerna.query.querystruct.selectors.QueryColumnSelector;

public class SecurityGroupUtils extends AbstractSecurityUtils {
	
	private static SecurityGroupUtils instance = new SecurityGroupUtils();
	private static final Logger logger = LogManager.getLogger(SecurityGroupUtils.class);

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

	// this class will control crud operations for the groups
	
	/**
	 * Add a new group
	 * @param groupId
	 * @param type
	 */
	public void addGroup(String groupId, String type, String description) {
		// need to ensure that the group is unique...
		
		
		String query = "INSERT INTO SMSS_GROUP (ID, TYPE, DESCRIPTION) VALUES (?,?,?)";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, groupId);
			ps.setString(parameterIndex++, type);
			ps.setString(parameterIndex++, description);
			ps.execute();
			ps.getConnection().commit();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(securityDb.isConnectionPooling()) {
				try {
					ps.getConnection().close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * Delete a new group
	 * @param groupId
	 * @param type
	 */
	public void deleteGroup(String groupId, String type) {
		String query = "DELETE FROM SMSS_GROUP (ID, TYPE) VALUES (?,?)";
		PreparedStatement ps = null;
		try {
			ps = securityDb.getPreparedStatement(query);
			int parameterIndex = 1;
			ps.setString(parameterIndex++, groupId);
			ps.setString(parameterIndex++, type);
			ps.execute();
			ps.getConnection().commit();
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if(ps != null) {
				try {
					ps.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(securityDb.isConnectionPooling()) {
				try {
					ps.getConnection().close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}
	
	/**
	 * Edit an existing group across all the tables
	 * @param groupId
	 * @param type
	 */
	public void editGroupAndPropagate(String curGroupId, String curType, String newGroupId, String newType) throws Exception{
		String[] queries = new String[] {
				"UPDATE SMSS_GROUP SET ID=?, TYPE=? WHERE ID=? AND TYPE=?",
				"UPDATE GROUPENGINEPERMISSION SET ID=?, TYPE=? WHERE ID=? AND TYPE=?",
				"UPDATE GROUPPROJECTPERMISSION SET ID=?, TYPE=? WHERE ID=? AND TYPE=?",
				"UPDATE GROUPINSIGHTPERMISSION SET ID=?, TYPE=? WHERE ID=? AND TYPE=?"
			};

		Connection conn = securityDb.makeConnection();
		boolean curAutoCommit = conn.getAutoCommit();
		try {
			if(curAutoCommit) {
				conn.setAutoCommit(false);
			}
			for(String query : queries) {
				PreparedStatement ps = null;
				try {
					ps = conn.prepareStatement(query);
					int parameterIndex = 1;
					ps.setString(parameterIndex++, newGroupId);
					ps.setString(parameterIndex++, newType);
					ps.setString(parameterIndex++, curGroupId);
					ps.setString(parameterIndex++, curType);
					ps.execute();
				} catch (SQLException e) {
					e.printStackTrace();
				} finally {
					if(ps != null) {
						try {
							ps.close();
						} catch (SQLException e) {
							e.printStackTrace();
						}
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			try {
				conn.commit();
			} catch (SQLException e1) {
				e1.printStackTrace();
			}
			if(curAutoCommit) {
				conn.setAutoCommit(curAutoCommit);
			}
			if(securityDb.isConnectionPooling()) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
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
