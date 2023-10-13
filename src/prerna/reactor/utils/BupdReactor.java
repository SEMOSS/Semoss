package prerna.reactor.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.reactor.AbstractReactor;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.util.Constants;
import prerna.util.Utility;

public class BupdReactor extends AbstractReactor {
	
	public BupdReactor() {
		this.keysToGet = new String[]{"fancy", "embed"};
	}

	@Override
	public NounMetadata execute() {
		organizeKeys();
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getDatabase(Constants.LOCAL_MASTER_DB);
		Connection conn = null;
		try {
			conn = engine.makeConnection();
		} catch (SQLException e) {
			e.printStackTrace();
			throw new IllegalArgumentException(e.getMessage());
		}
		
		Statement stmt = null;
		try {
			// check to see if such a fancy name exists
			stmt = conn.createStatement();
			String query = "SELECT embed, fancy from bitly where fancy='" + this.keyValue.get("fancy") + "'";
			ResultSet rs = stmt.executeQuery(query);
			// if there is a has next not sure what
			
			if(rs.next())
			{
				query = "Update bitly set embed = '" +  this.keyValue.get("embed") + "' where fancy = '" + this.keyValue.get("fancy") + "'";
				stmt.executeUpdate(query);
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} finally {
			if(stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
			if(engine.isConnectionPooling() && conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return new NounMetadata("Updated " + this.keyValue.get("fancy"), PixelDataType.CONST_STRING);
	}
}
