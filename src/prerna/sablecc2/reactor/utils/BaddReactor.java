package prerna.sablecc2.reactor.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.PixelOperationType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.Utility;

public class BaddReactor extends AbstractReactor {
	
	public BaddReactor() {
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
		String errorMessage = "";
		Statement stmt = null;
		try {
			// check to see if such a fancy name exists
			stmt = conn.createStatement();
			String query = "SELECT embed, fancy from bitly where fancy='" + this.keyValue.get("fancy") + "'";
			ResultSet rs = stmt.executeQuery(query);
			// if there is a has next not sure what
			
			if(rs.next())
			{
				errorMessage = "Name " + this.keyValue.get("fancy") + " already exists. Please enter a new name.";
			}
			else
			{
				query = "Insert into bitly(embed, fancy) values ('" +  this.keyValue.get("embed") + "' , '" + this.keyValue.get("fancy") + "')";
				stmt.execute(query);
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
		
		if(errorMessage.isEmpty()) {
			return new NounMetadata("Added " + this.keyValue.get("fancy"), PixelDataType.CONST_STRING);
		} else {
			System.out.println(errorMessage);
			return new NounMetadata(errorMessage, PixelDataType.CONST_STRING, PixelOperationType.ERROR);
		}
	}
}
