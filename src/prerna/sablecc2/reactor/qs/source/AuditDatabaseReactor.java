package prerna.sablecc2.reactor.qs.source;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Vector;

import prerna.auth.utils.AbstractSecurityUtils;
import prerna.auth.utils.SecurityQueryUtils;
import prerna.ds.h2.H2Frame;
import prerna.engine.impl.rdbms.AuditDatabase;
import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.nameserver.utility.MasterDatabaseUtility;
import prerna.sablecc2.om.GenRowStruct;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.ReactorKeysEnum;
import prerna.sablecc2.om.execptions.SemossPixelException;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.ConnectionUtils;
import prerna.util.Utility;

public class AuditDatabaseReactor extends AbstractReactor {

	public AuditDatabaseReactor() {
		this.keysToGet = new String[] { 
				ReactorKeysEnum.APP.getKey(), 
				ReactorKeysEnum.TABLES.getKey(),
				ReactorKeysEnum.COLUMNS.getKey(), 
				ReactorKeysEnum.DATE_TIME_FIELD.getKey(),
				ReactorKeysEnum.VALUE.getKey()
				};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		String appId = this.keyValue.get(ReactorKeysEnum.APP.getKey());
		// we may have the alias
		if (AbstractSecurityUtils.securityEnabled()) {
			appId = SecurityQueryUtils.testUserEngineIdForAlias(this.insight.getUser(), appId);
			if (!SecurityQueryUtils.userCanViewEngine(this.insight.getUser(), appId)) {
				throw new IllegalArgumentException(
						"Database " + appId + " does not exist or user does not have access to database");
			}
		} else {
			appId = MasterDatabaseUtility.testEngineIdIfAlias(appId);
			if (!MasterDatabaseUtility.getAllEngineIds().contains(appId)) {
				throw new IllegalArgumentException("Database " + appId + " does not exist");
			}
		}

		if (!(Utility.getEngine(appId) instanceof RDBMSNativeEngine)) {
			throw new IllegalArgumentException("App must be using a relational database");
		}
		// process table filters
		String tableFilterSyntax = generateFilterSyntax(ReactorKeysEnum.TABLES.getKey());
		// process column filters
		String columnFilterSyntax = generateFilterSyntax(ReactorKeysEnum.COLUMNS.getKey());
		String dateTimeField = this.keyValue.get(ReactorKeysEnum.DATE_TIME_FIELD.getKey());
		String dateDiff = this.keyValue.get(ReactorKeysEnum.VALUE.getKey());

		// get audit database from app id
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(appId);
		AuditDatabase audit = engine.generateAudit();
		Connection conn = null;

		HashSet<String> userIdSet = new HashSet<String>();
		Statement stmt = null;
		ResultSet rs = null;

		String[] headers = new String[] { "Modification_Date", "Id", "Modification_Type", "Altered_Table", "Key_Column", "Key_Column_Value","Altered_Column", "Old_Value", "New_Value", "User_Email" };
		String[] types = new String[] { "TIMESTAMP", "STRING", "STRING", "STRING", "STRING", "STRING", "STRING","STRING", "STRING", "STRING" };
		H2Frame frame = new H2Frame(headers, types);
		// create prepared statement to insert data into frame
		PreparedStatement insertPS = frame.getBuilder().createInsertPreparedStatement(frame.getName(), headers);
		// create prepared statement to update frame user ids to user emails
		PreparedStatement updatePS = frame.createUpdatePreparedStatement(new String[] { "USER_EMAIL" }, new String[] { "USER_EMAIL" });
		try {
			// create query with specified parameters
			StringBuilder sql = new StringBuilder();
			sql.append(
					"SELECT timestamp, id, type, table, key_column, key_column_value, altered_column, old_value, new_value, user FROM AUDIT_TABLE ");
			// add table and column filters
			sql.append(" WHERE table in " + tableFilterSyntax + " AND altered_column IN " + columnFilterSyntax + " ");
			// add time filters
			if (dateTimeField != null && dateDiff != null) {
				sql.append(" AND TIMESTAMP > DATEADD('" + dateTimeField + "',-" + dateDiff + ", CURRENT_DATE)");
			}
			// end sql staement
			sql.append(";");
			
			// query audit database
			conn = audit.getConnection();
			stmt = conn.createStatement();
			rs = stmt.executeQuery(sql.toString());
			// grab values and insert into frame
			while (rs.next()) {
				int i = 1;
				insertPS.setTimestamp(i, rs.getTimestamp(i++));
				// id
				insertPS.setString(i, rs.getString(i++));
				// type
				insertPS.setString(i, rs.getString(i++));
				// table
				insertPS.setString(i, rs.getString(i++));
				// keyCol
				insertPS.setString(i, rs.getString(i++));
				// keyColVal
				insertPS.setString(i, rs.getString(i++));
				// altered col
				insertPS.setString(i, rs.getString(i++));
				// oldVal
				insertPS.setString(i, rs.getString(i++));
				// newVal
				insertPS.setString(i, rs.getString(i++));
				// user id
				String userId = rs.getString(i);
				// store user ids to translate
				userIdSet.add(userId);
				insertPS.setString(i, userId);
				insertPS.addBatch();
			}
			insertPS.executeBatch();
			if (userIdSet.isEmpty()) {
				String errorMsg = "No modifications have been made with the specified parameters.";
				NounMetadata noun = new NounMetadata(errorMsg, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
				SemossPixelException err = new SemossPixelException(noun);
				err.setContinueThreadOfExecution(false);
				throw err;
			}
			// get user info from ids
			List<String> userIds = new Vector<>(userIdSet);
			Map<String, Map<String, Object>> userInfo = SecurityQueryUtils.getUserInfo(userIds);
			// update user ids to user emails
			for (String userId : userInfo.keySet()) {
				updatePS.setObject(1, userInfo.get(userId).get("EMAIL"));
				updatePS.setObject(2, userId);
				updatePS.addBatch();
			}
			updatePS.executeBatch();
		} catch (SQLException e) {
			e.printStackTrace();
			String errorMsg = "An error occured while retrieving data from the audit database";
			NounMetadata noun = new NounMetadata(errorMsg, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
			SemossPixelException err = new SemossPixelException(noun);
			err.setContinueThreadOfExecution(false);
			throw err;
		} finally {
			ConnectionUtils.closeAllConnections(null, rs, stmt);
			ConnectionUtils.closePreparedStatement(insertPS);
			ConnectionUtils.closePreparedStatement(updatePS);
		}

		this.insight.setDataMaker(frame);
		NounMetadata retNoun = new NounMetadata(frame, PixelDataType.FRAME, PixelOperationType.FRAME_HEADERS_CHANGE, PixelOperationType.FRAME_DATA_CHANGE);
		return retNoun;
	}

	private String generateFilterSyntax(String key) {
		GenRowStruct grs = this.store.getNoun(key);
		StringBuilder filterSyntax = new StringBuilder("(");
		if (grs != null && !grs.isEmpty()) {
			for (int i = 0; i < grs.size(); i++) {
				if (i > 0) {
					filterSyntax.append(",");
				}
				filterSyntax.append("'" + grs.get(i).toString() + "'");
			}
		}
		filterSyntax.append(")");
		return filterSyntax.toString();
	}
}