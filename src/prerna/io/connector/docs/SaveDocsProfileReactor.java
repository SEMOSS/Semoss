package prerna.io.connector.docs;

import java.util.HashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.sql.*;

import prerna.auth.User;
import prerna.auth.utils.AdminSecurityGroupUtils;
import prerna.engine.api.IRawSelectWrapper;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.query.querystruct.SelectQueryStruct;
import prerna.query.querystruct.filters.SimpleQueryFilter;
import prerna.query.querystruct.selectors.QueryColumnSelector;
import prerna.rdf.engine.wrappers.WrapperManager;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class SaveDocsProfileReactor extends AbstractReactor {

	private static final Logger classLogger = LogManager.getLogger(AdminSecurityGroupUtils.class);
	static RDBMSNativeEngine securityDb = (RDBMSNativeEngine) Utility.getDatabase(Constants.SECURITY_DB);

	public SaveDocsProfileReactor() {
		this.keysToGet = new String[] { ReactorKeysEnum.NAME.getKey() };
		this.keyRequired = new int[] { 1 };
	}

	@Override
	public NounMetadata execute() {
		this.organizeKeys();
		String name = this.keyValue.get(this.keysToGet[0]);
		if (nameExists(name)) {
			throw new IllegalArgumentException("Name " + name + " already exist");
		}
		Boolean insertData = false;
		Timestamp date = new Timestamp(System.currentTimeMillis());
		User user = this.insight.getUser();
		String google_username = user.getPrimaryLoginToken().getName();
		String google_userid = user.getPrimaryLoginToken().getEmail();
		HashMap<Object, Object> map = new HashMap<Object, Object>();
		int profileId = 0;
		String ID = "id";
		String SUCCESS = "Success";
		Connection conn = null;
		try {
			conn = securityDb.makeConnection();
			insertData = insertData(conn, google_username, name, date, google_userid);
			if (insertData) {
				profileId = readData(conn, name, google_userid, date);
			}
			if (profileId != 0) {
				map.put(ID, profileId);
				map.put(SUCCESS, insertData);
			}
			return new NounMetadata(map, PixelDataType.CUSTOM_DATA_STRUCTURE, PixelOperationType.OPERATION);
		} catch (Exception e) {
			e.printStackTrace();
			String error = "Error in executing the reactor";
			return new NounMetadata(error + ":" + e.getMessage(), PixelDataType.CUSTOM_DATA_STRUCTURE,
					PixelOperationType.OPERATION);
		}
	}

	private Boolean insertData(Connection conn, String google_username, String name, Timestamp date,
			String google_userid) {
		String maxIdQuery = "SELECT MAX(ID) FROM GOOGLE_DOCS_PROFILE";
		int nextId = 1;

		try (Statement stmt = conn.createStatement(); ResultSet rs = stmt.executeQuery(maxIdQuery)) {
			if (rs.next()) {
				String maxIdStr = rs.getString(1);
				if (maxIdStr != null && !maxIdStr.trim().isEmpty()) {
					try {
						nextId = Integer.parseInt(maxIdStr.trim()) + 1;
					} catch (NumberFormatException e) {
						nextId = 1;
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			nextId = 1;
		}

		String newId = String.valueOf(nextId);
		String type = "CONNECTOR";
		String insertQuery = "INSERT INTO GOOGLE_DOCS_PROFILE (ID, TYPE, USERNAME, NAME, DATECREATED, USERID) VALUES (?, ?, ?, ?, ?, ?)";

		try (PreparedStatement pstmt = conn.prepareStatement(insertQuery)) {
			pstmt.setString(1, newId);
			pstmt.setString(2, type);
			pstmt.setString(3, google_username);
			pstmt.setString(4, name);
			pstmt.setTimestamp(5, date);
			pstmt.setString(6, google_userid);
			pstmt.executeUpdate();
			return true;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		} finally {
			if (securityDb.isConnectionPooling() && conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
	}

	private Integer readData(Connection conn, String Name, String google_userid, Timestamp date) {

		int profileKey = 0;
		String newprofileKey = null;
		String query = "SELECT ID FROM GOOGLE_DOCS_PROFILE WHERE NAME=? AND USERID=? AND DATECREATED=? ";
		try (PreparedStatement pstmt = conn.prepareStatement(query)) {
			pstmt.setString(1, Name);
			pstmt.setString(2, google_userid);
			pstmt.setTimestamp(3, date);
			try (ResultSet rs = pstmt.executeQuery()) {
				if (rs.next()) {
					newprofileKey = rs.getString("ID");
					profileKey = Integer.parseInt(newprofileKey);
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (securityDb.isConnectionPooling() && conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}
		return profileKey;
	}

	public static boolean nameExists(String name) {
		SelectQueryStruct qs = new SelectQueryStruct();
		qs.addSelector(new QueryColumnSelector("GOOGLE_DOCS_PROFILE__NAME"));
		qs.addExplicitFilter(SimpleQueryFilter.makeColToValFilter("GOOGLE_DOCS_PROFILE__NAME", "==", name));
		IRawSelectWrapper wrapper = null;
		try {
			wrapper = WrapperManager.getInstance().getRawWrapper(securityDb, qs);
			if (wrapper.hasNext()) {
				return true;
			}
		} catch (Exception e) {
			classLogger.error(Constants.STACKTRACE, e);
		} finally {
			if (wrapper != null) {
				try {
					wrapper.close();
				} catch (IOException e) {
					classLogger.error(Constants.STACKTRACE, e);
				}
			}
		}

		return false;
	}

	@Override
	public String getReactorDescription() {
		return "This reactor insert the data into the googledocsprofile database and also return the id.";
	}

	@Override
	protected String getDescriptionForKey(String key) {
		if (key.equals(ReactorKeysEnum.NAME.getKey())) {
			return "Name " + ReactorKeysEnum.NAME.getKey();
		} 
		return super.getDescriptionForKey(key);
	}

}