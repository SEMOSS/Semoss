package prerna.sablecc2.reactor.utils;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import prerna.engine.impl.rdbms.RDBMSNativeEngine;
import prerna.sablecc2.om.PixelDataType;
import prerna.sablecc2.om.nounmeta.NounMetadata;
import prerna.sablecc2.reactor.AbstractReactor;
import prerna.util.Constants;
import prerna.util.Utility;

public class BitlyUpdateReactor extends AbstractReactor {
	
	public BitlyUpdateReactor() {
		this.keysToGet = new String[]{"fancy", "embed"};
	}

	@Override
	public NounMetadata execute() {

		organizeKeys();
		RDBMSNativeEngine engine = (RDBMSNativeEngine) Utility.getEngine(Constants.LOCAL_MASTER_DB_NAME);
		Connection conn = engine.makeConnection();
		
		try {
			// check to see if such a fancy name exists
			Statement stmt = conn.createStatement();
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
		}
		return new NounMetadata("Updated " + this.keyValue.get("fancy"), PixelDataType.CONST_STRING);
	}
}
